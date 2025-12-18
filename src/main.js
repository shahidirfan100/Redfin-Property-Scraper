import { Actor, log } from 'apify';
import { Dataset, gotScraping } from 'crawlee';
import { chromium } from 'playwright';
import { load as cheerioLoad } from 'cheerio';

// ============================================================================
// CONSTANTS
// ============================================================================

const REDFIN_BASE = 'https://www.redfin.com';
const REDFIN_API_GIS = `${REDFIN_BASE}/stingray/api/gis`;
const JSON_PREFIX = /^{}&&/;
const REGION_TYPE_MAP = { city: 6, zipcode: 5, neighborhood: 4, county: 2, state: 1 };
const BLOCK_STATUS = new Set([403, 429, 503]);
const DETAIL_BLOCK_PATTERNS = [/captcha/i, /unusual traffic/i, /access denied/i];
const DEFAULT_TIMEOUT_MS = 35000;

const USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0',
];

// ============================================================================
// HELPERS
// ============================================================================

const pickUserAgent = () => USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)];
const delay = (ms) => new Promise((res) => setTimeout(res, ms));
const randBetween = (min, max) => Math.floor(Math.random() * (max - min + 1)) + min;
const ensureAbsoluteUrl = (url) => {
    if (!url) return null;
    if (url.startsWith('http')) return url;
    return `${REDFIN_BASE}${url.startsWith('/') ? '' : '/'}${url}`;
};

const buildStealthHeaders = (ua, referer = REDFIN_BASE) => ({
    'User-Agent': ua,
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
    'Cache-Control': 'no-cache',
    'Pragma': 'no-cache',
    'Referer': referer,
    'Sec-Ch-Ua': '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
    'Sec-Ch-Ua-Mobile': '?0',
    'Sec-Ch-Ua-Platform': '"Windows"',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'same-origin',
    'Upgrade-Insecure-Requests': '1',
});

const buildApiHeaders = (ua, referer = REDFIN_BASE) => ({
    'User-Agent': ua,
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'Accept-Language': 'en-US,en;q=0.9',
    'Content-Type': 'application/json;charset=UTF-8',
    'X-Requested-With': 'XMLHttpRequest',
    'Referer': referer,
    'Cache-Control': 'no-cache',
});

const safeJsonParse = (text) => {
    try {
        return JSON.parse(text);
    } catch {
        return null;
    }
};

const decodeJsonString = (encoded) => encoded.replace(/\\"/g, '"').replace(/\\\\/g, '\\');

const cleanText = (text) => (text ? text.replace(/\s+/g, ' ').trim() : null);

const createLimiter = (maxConcurrency) => {
    let active = 0;
    const queue = [];
    const next = () => {
        if (active >= maxConcurrency || queue.length === 0) return;
        active += 1;
        const { task, resolve, reject } = queue.shift();
        task()
            .then(resolve)
            .catch(reject)
            .finally(() => {
                active -= 1;
                next();
            });
    };
    return (task) =>
        new Promise((resolve, reject) => {
            queue.push({ task, resolve, reject });
            next();
        });
};

const deriveRegionMeta = (url, explicitRegionId, explicitRegionType) => {
    if (!url) return null;
    try {
        const urlObj = new URL(url);
        const match = urlObj.pathname.match(/\/(city|zipcode|neighborhood|county|state)\/(\d+)\//i);
        const regionId = explicitRegionId || (match ? match[2] : null);
        const regionType = explicitRegionType || (match ? REGION_TYPE_MAP[match[1].toLowerCase()] : 6);
        const slug = urlObj.pathname.split('/').filter(Boolean).pop() || '';
        const market = slug.replace(/[^a-z0-9]+/gi, '-').toLowerCase() || 'market';
        return { url: urlObj.toString(), regionId, regionType, market };
    } catch {
        return null;
    }
};

const requestWithRetry = async (fn, { label, maxRetries = 2, baseDelay = 1500 }) => {
    let attempt = 0;
    let lastError;
    while (attempt <= maxRetries) {
        try {
            return await fn(attempt + 1);
        } catch (err) {
            lastError = err;
            if (attempt >= maxRetries) break;
            const wait = Math.round(baseDelay * Math.pow(1.5, attempt) + randBetween(100, 600));
            log.warning(`${label || 'request'} failed (attempt ${attempt + 1}/${maxRetries + 1}): ${err.message}. Retrying in ${wait} ms`);
            await delay(wait);
        }
        attempt += 1;
    }
    throw lastError;
};

// ============================================================================
// PARSERS
// ============================================================================

const extractJsonLd = (html) => {
    const $ = cheerioLoad(html);
    const jsonLdData = [];
    $('script[type="application/ld+json"]').each((_, el) => {
        const raw = $(el).contents().text().trim();
        const parsed = safeJsonParse(raw);
        if (!parsed) return;
        if (Array.isArray(parsed)) jsonLdData.push(...parsed);
        else jsonLdData.push(parsed);
    });
    return jsonLdData;
};

const parsePropertyFromJsonLd = (jsonLdArray) => {
    if (!Array.isArray(jsonLdArray)) return null;
    const property = jsonLdArray.find(
        (item) =>
            item['@type'] === 'Product' ||
            item['@type'] === 'Apartment' ||
            item['@type'] === 'SingleFamilyResidence' ||
            item['@type'] === 'House'
    );
    if (!property) return null;

    return {
        title: property.name || null,
        price: property.offers?.price || property.offers?.[0]?.price || null,
        address: property.address?.streetAddress || null,
        city: property.address?.addressLocality || null,
        state: property.address?.addressRegion || null,
        zip: property.address?.postalCode || null,
        description: property.description || null,
        image: property.image?.[0]?.url || property.image?.url || null,
        latitude: property.geo?.latitude || null,
        longitude: property.geo?.longitude || null,
    };
};

const parseHomesFromApi = (payload) => {
    const homes = payload?.payload?.homes || [];
    return homes.map((home) => ({
        propertyId: home.propertyId || home.mlsId?.value,
        url: ensureAbsoluteUrl(home.url),
        address: home.streetLine?.value || home.address || null,
        city: home.city,
        state: home.state,
        zip: home.zip,
        price: home.priceInfo?.amount || home.price,
        beds: home.beds,
        baths: home.baths,
        sqft: home.sqFt || home.sqft,
        lotSize: home.lotSize?.amount,
        propertyType: home.propertyType,
        status: home.mlsStatus?.value || home.status,
        listingDate: home.listingDate,
        latitude: home.latLong?.latitude || home.lat,
        longitude: home.latLong?.longitude || home.lng,
        mlsNumber: home.mlsNumber || home.mlsId?.value,
    }));
};

const parseEmbeddedHomes = (html) => {
    const patterns = [
        /window\.__REDWOOD__\s*=\s*JSON\.parse\('(?<json>[^']+)'/s,
        /window\.__REDWOOD__\s*=\s*(?<json>{[\s\S]*?})<\/script>/,
        /window\.__PRELOADED_STATE__\s*=\s*(?<json>{[\s\S]*?})<\/script>/,
    ];
    for (const pattern of patterns) {
        const match = html.match(pattern);
        if (!match?.groups?.json) continue;
        const rawJson = pattern.source.includes('JSON\\.parse') ? decodeJsonString(match.groups.json) : match.groups.json;
        const parsed = safeJsonParse(rawJson);
        if (!parsed) continue;

        const homes = parsed?.payload?.homes || parsed?.homes || parsed?.properties;
        if (Array.isArray(homes) && homes.length > 0) {
            return homes.map((home) => ({
                propertyId: home.propertyId || home.id || home.mlsId?.value,
                url: ensureAbsoluteUrl(home.url),
                address: home.address || home.streetLine?.value || null,
                city: home.city,
                state: home.state,
                zip: home.zip,
                price: home.priceInfo?.amount || home.price,
                beds: home.beds,
                baths: home.baths,
                sqft: home.sqFt || home.sqft,
                lotSize: home.lotSize?.amount,
                propertyType: home.propertyType,
                status: home.status || home.mlsStatus?.value,
                listingDate: home.listingDate,
                latitude: home.latLong?.latitude || home.lat,
                longitude: home.latLong?.longitude || home.lng,
                mlsNumber: home.mlsNumber || home.mlsId?.value,
            }));
        }
    }
    return [];
};

const parseHtmlListPage = (html) => {
    const $ = cheerioLoad(html);
    const listings = [];
    const cards = $('[data-rf-test-id="abp-card"], [data-rf-test-name="basic-card"], .HomeCardContainer');

    cards.each((_, el) => {
        const $el = $(el);
        const url = ensureAbsoluteUrl($el.find('a').attr('href'));
        const price = cleanText(
            $el.find('[data-rf-test-id="abp-price"], [data-rf-test-name="homecard-price"], .homecardV2Price').first().text()
        );
        const address = cleanText(
            $el.find('[data-rf-test-id="abp-streetLine"], .homeAddressV2, [data-rf-test-name="homecard-address"]').first().text()
        );
        const cityStateZip = cleanText($el.find('[data-rf-test-id="abp-cityStateZip"], .homeAddressV2').last().text());

        let city;
        let state;
        let zip;
        if (cityStateZip) {
            const parts = cityStateZip.split(',');
            city = cleanText(parts[0]);
            const stateZip = parts[1] ? parts[1].trim().split(' ') : [];
            state = stateZip[0] || null;
            zip = stateZip[1] || null;
        }

        const beds = cleanText($el.find('[data-rf-test-id="abp-beds"]').text());
        const baths = cleanText($el.find('[data-rf-test-id="abp-baths"]').text());
        const sqft = cleanText($el.find('[data-rf-test-id="abp-sqft"]').text());

        const propertyId = $el.attr('data-property-id') || $el.data('property-id');

        if (url || propertyId) {
            listings.push({
                propertyId,
                url,
                address,
                city,
                state,
                zip,
                price,
                beds,
                baths,
                sqft,
            });
        }
    });

    if (listings.length === 0) {
        const embedded = parseEmbeddedHomes(html);
        listings.push(...embedded);
    }

    return listings;
};

const parseEmbeddedDetail = (html) => {
    const patterns = [
        /window\.__REDWOOD__\s*=\s*JSON\.parse\('(?<json>[^']+)'/s,
        /window\.__REDWOOD__\s*=\s*(?<json>{[\s\S]*?})<\/script>/,
        /window\.__PRELOADED_STATE__\s*=\s*(?<json>{[\s\S]*?})<\/script>/,
        /window\.__INITIAL_STATE__\s*=\s*(?<json>{[\s\S]*?})<\/script>/,
        /window\.__REDFIN_STATE__\s*=\s*(?<json>{[\s\S]*?})<\/script>/,
    ];

    for (const pattern of patterns) {
        const match = html.match(pattern);
        if (!match?.groups?.json) continue;
        const rawJson = pattern.source.includes('JSON\\.parse') ? decodeJsonString(match.groups.json) : match.groups.json;
        const parsed = safeJsonParse(rawJson);
        if (!parsed) continue;

        const home =
            parsed?.homeInfo ||
            parsed?.propertyDetailsInfo?.propertyInfo ||
            parsed?.propertyInfo ||
            parsed?.property ||
            parsed?.payload?.homeDetail ||
            parsed?.payload?.propertyInfo ||
            parsed?.initialState?.homeInfo;

        if (!home) continue;

        return {
            title: home.name || home.shortAddress || home.formattedAddress,
            price: home.price || home.latestPrice || home.priceInfo?.amount,
            beds: home.beds || home.bedrooms,
            baths: home.baths || home.bathrooms,
            sqft: home.sqFt || home.sqft || home.squareFeet,
            address: home.streetLine || home.streetAddress || home.address?.streetAddress,
            city: home.city || home.address?.city,
            state: home.state || home.address?.state,
            zip: home.zip || home.address?.zip,
            description: home.description || home.publicRemarks || home.remarks,
            latitude: home.latitude || home.latLong?.latitude || home.lat,
            longitude: home.longitude || home.latLong?.longitude || home.lng,
            lotSize: home.lotSize || home.lotSizeSqFt || home.lotSizeInSqFt || home.lotSize?.value,
            yearBuilt: home.yearBuilt,
            hoa: home.hoa || home.hoaFee || home.hoaDues,
            status: home.status || home.mlsStatus || home.propertyStatus,
            listingDate: home.listingDate || home.listedOnDate || home.listDate,
            mlsNumber: home.mlsId || home.mlsNumber,
            propertyType: home.propertyType,
            id: home.id || home.propertyId,
        };
    }

    return null;
};

const parseHtmlDetail = (html) => {
    const $ = cheerioLoad(html);
    const jsonLdArray = extractJsonLd(html);
    const propertyData = parsePropertyFromJsonLd(jsonLdArray) || parseEmbeddedDetail(html) || {};

    const detailText = (selector) => cleanText($(selector).first().text());

    const descriptionMeta = $('meta[name="description"]').attr('content');

    return {
        title: propertyData.title || detailText('h1') || detailText('[data-rf-test-id="abp-h1"]'),
        price: propertyData.price || detailText('[data-rf-test-id="abp-price"]') || detailText('.statsValue'),
        beds: propertyData.beds || detailText('[data-rf-test-id="abp-beds"]'),
        baths: propertyData.baths || detailText('[data-rf-test-id="abp-baths"]'),
        sqft: propertyData.sqft || detailText('[data-rf-test-id="abp-sqft"]'),
        address: propertyData.address || detailText('[data-rf-test-id="abp-streetLine"]') || detailText('.full-address'),
        city: propertyData.city,
        state: propertyData.state,
        zip: propertyData.zip,
        description:
            propertyData.description ||
            cleanText(descriptionMeta) ||
            detailText('.remarks') ||
            detailText('[data-rf-test-id="abp-description"]') ||
            cleanText($('.propertyDescription').text()),
        latitude: propertyData.latitude,
        longitude: propertyData.longitude,
        lotSize: propertyData.lotSize || detailText('[data-rf-test-id="lot-size"]'),
        yearBuilt: propertyData.yearBuilt || detailText('[data-rf-test-id="year-built"]'),
        hoa: propertyData.hoa || detailText('[data-rf-test-id="hoa-dues"]'),
        status: propertyData.status || detailText('[data-rf-test-id="abp-status"]'),
        listingDate: propertyData.listingDate || detailText('[data-rf-test-id="listing-date"]'),
        mlsNumber: propertyData.mlsNumber || detailText('[data-rf-test-id="mls-number"]'),
    };
};

const buildProperty = ({ listing = {}, detail = {}, source }) => {
    const propertyId = listing.propertyId || detail.id || listing.mlsNumber;
    const url = ensureAbsoluteUrl(listing.url || detail.url) || (propertyId ? `${REDFIN_BASE}/home/${propertyId}` : null);
    const priceVal = detail.price || listing.price;
    const bedsVal = detail.beds || listing.beds;
    const bathsVal = detail.baths || listing.baths;
    const sqftVal = detail.sqft || listing.sqft;

    const normalizeNumber = (value) => {
        if (value === null || value === undefined) return null;
        if (typeof value === 'number') return value;
        const numeric = String(value).replace(/[^\d.]/g, '');
        return numeric ? Number(numeric) : null;
    };

    const formattedPrice =
        typeof priceVal === 'number'
            ? `$${priceVal.toLocaleString()}`
            : typeof priceVal === 'string' && priceVal.startsWith('$')
              ? priceVal
              : priceVal
                ? `$${normalizeNumber(priceVal)?.toLocaleString() || priceVal}`
                : null;

    const address = detail.address || listing.address;
    const city = detail.city || listing.city;
    const state = detail.state || listing.state;
    const zip = detail.zip || listing.zip;
    const fullAddress = address && city && state ? `${address}, ${city}, ${state}${zip ? ` ${zip}` : ''}` : address || null;

    return {
        propertyId: propertyId || url,
        url,
        address: fullAddress,
        streetAddress: address,
        city,
        state,
        zip,
        price: formattedPrice,
        beds: normalizeNumber(bedsVal),
        baths: normalizeNumber(bathsVal),
        sqft: normalizeNumber(sqftVal),
        propertyType: listing.propertyType || detail.propertyType || null,
        status: detail.status || listing.status || null,
        listingDate: detail.listingDate || listing.listingDate || null,
        description: detail.description || null,
        latitude: detail.latitude || listing.latitude || null,
        longitude: detail.longitude || listing.longitude || null,
        mlsNumber: detail.mlsNumber || listing.mlsNumber || null,
        lotSize: detail.lotSize || listing.lotSize || null,
        yearBuilt: detail.yearBuilt || listing.yearBuilt || null,
        hoa: detail.hoa || listing.hoa || null,
        source,
        fetched_at: new Date().toISOString(),
    };
};

// ============================================================================
// FETCHERS
// ============================================================================

const fetchJsonApiPage = async ({ target, page, pageSize, proxyConfiguration, timeoutMs, maxRetries }) => {
    const params = new URLSearchParams({
        al: 1,
        market: target.market || 'market',
        num_homes: pageSize,
        page_number: page,
        region_id: target.regionId,
        region_type: target.regionType,
        status: 9,
        uipt: '1,2,3,4,5,6,7,8,9',
        sf: '1,2,3,4,5,6,7,8,9',
        v: 8,
    });
    const url = `${REDFIN_API_GIS}?${params.toString()}`;

    return requestWithRetry(
        async () => {
            const res = await gotScraping({
                url,
                responseType: 'text',
                headers: buildApiHeaders(pickUserAgent(), target.url),
                proxyUrl: proxyConfiguration ? await proxyConfiguration.newUrl() : undefined,
                timeout: { request: timeoutMs || DEFAULT_TIMEOUT_MS },
                throwHttpErrors: false,
                retry: { limit: 0 },
            });

            if (BLOCK_STATUS.has(res.statusCode)) {
                throw new Error(`Blocked (${res.statusCode})`);
            }

            if (res.statusCode !== 200) {
                throw new Error(`Unexpected status ${res.statusCode}`);
            }

            const cleaned = res.body.replace(JSON_PREFIX, '').trim();
            const parsed = safeJsonParse(cleaned);
            if (!parsed?.payload?.homes) {
                throw new Error('Missing homes payload');
            }
            return parsed;
        },
        { label: `json-api p${page}`, maxRetries }
    );
};

const fetchSearchHtml = async ({ url, proxyConfiguration, timeoutMs, maxRetries }) =>
    requestWithRetry(
        async () => {
            const res = await gotScraping({
                url,
                responseType: 'text',
                headers: buildStealthHeaders(pickUserAgent(), url),
                proxyUrl: proxyConfiguration ? await proxyConfiguration.newUrl() : undefined,
                timeout: { request: timeoutMs || DEFAULT_TIMEOUT_MS },
                throwHttpErrors: false,
                retry: { limit: 0 },
            });

            if (BLOCK_STATUS.has(res.statusCode)) {
                throw new Error(`Blocked (${res.statusCode})`);
            }

            if (res.statusCode !== 200) {
                throw new Error(`Unexpected status ${res.statusCode}`);
            }

            const body = res.body || '';
            if (DETAIL_BLOCK_PATTERNS.some((pattern) => pattern.test(body))) {
                throw new Error('HTML response indicates blocking');
            }

            return body;
        },
        { label: `html ${url}`, maxRetries }
    );

const fetchDetailPage = async ({ url, proxyConfiguration, timeoutMs, maxRetries }) => {
    try {
        const html = await fetchSearchHtml({ url: ensureAbsoluteUrl(url), proxyConfiguration, timeoutMs, maxRetries });
        return parseHtmlDetail(html);
    } catch (err) {
        log.warning(`Detail fetch failed for ${url}: ${err.message}`);
        return {};
    }
};

const createStealthyBrowser = async (proxyConfiguration) => {
    const launchOptions = {
        headless: true,
        args: [
            '--disable-blink-features=AutomationControlled',
            '--disable-dev-shm-usage',
            '--disable-gpu',
            '--no-first-run',
            '--no-default-browser-check',
        ],
    };

    if (proxyConfiguration) {
        const proxyUrl = await proxyConfiguration.newUrl();
        launchOptions.proxy = { server: proxyUrl };
    }

    return chromium.launch(launchOptions);
};

const fetchViaPlaywright = async ({ url, proxyConfiguration }) => {
    let browser;
    let context;
    let page;

    try {
        browser = await createStealthyBrowser(proxyConfiguration);
        context = await browser.newContext({
            userAgent: pickUserAgent(),
            viewport: { width: 1366, height: 768 },
            ignoreHTTPSErrors: true,
            extraHTTPHeaders: buildStealthHeaders(pickUserAgent(), url),
        });

        await context.addInitScript(() => {
            Object.defineProperty(navigator, 'webdriver', { get: () => false });
        });

        page = await context.newPage();
        await page.goto(url, { waitUntil: 'networkidle', timeout: DEFAULT_TIMEOUT_MS });
        await page.waitForTimeout(randBetween(1500, 3000));

        const content = await page.content();
        const listings = parseHtmlListPage(content);
        return listings;
    } catch (err) {
        log.warning(`Playwright fallback failed: ${err.message}`);
        return [];
    } finally {
        if (page) await page.close().catch(() => {});
        if (context) await context.close().catch(() => {});
        if (browser) await browser.close().catch(() => {});
    }
};

// ============================================================================
// MAIN
// ============================================================================

await Actor.init();

try {
    const input = (await Actor.getInput()) || {};
    const {
        startUrl,
        regionId,
        regionType,
        collectDetails = true,
        results_wanted: resultsWantedRaw = 50,
        max_pages: maxPagesRaw = 3,
        maxConcurrency = 3,
        proxyConfiguration,
    } = input;

    const preferJson = input.preferJson ?? true; // internal default
    const useHtmlFallback = input.useHtmlFallback ?? true;
    const usePlaywright = input.usePlaywright ?? false;
    const pageSize = input.pageSize ?? 200;
    const maxRetries = input.maxRetries ?? 2;
    const requestTimeoutMs = input.requestTimeoutMs ?? DEFAULT_TIMEOUT_MS;
    const delayMinMs = input.delayMinMs ?? 350;
    const delayMaxMs = input.delayMaxMs ?? 1200;

    const resultsWanted = Math.max(1, Number.isFinite(+resultsWantedRaw) ? +resultsWantedRaw : 1);
    const maxPages = Math.max(1, Number.isFinite(+maxPagesRaw) ? +maxPagesRaw : 1);
    const proxyConf = proxyConfiguration ? await Actor.createProxyConfiguration({ ...proxyConfiguration }) : undefined;

    const uniqueTargetUrls = startUrl ? [startUrl] : [];

    if (!uniqueTargetUrls.length) {
        throw new Error('Provide startUrl.');
    }

    const limiter = createLimiter(Math.max(1, Math.min(10, maxConcurrency)));
    const seen = new Set();
    let saved = 0;
    const stats = { apiPages: 0, htmlPages: 0, errors: 0, methodsUsed: new Set() };
    const startTime = Date.now();

    log.info('Starting Redfin Property Scraper (JSON-first, HTML fallback)');

    for (const targetUrl of uniqueTargetUrls) {
        if (saved >= resultsWanted) break;

        const regionMeta = deriveRegionMeta(targetUrl, regionId, regionType);
        if (!regionMeta?.regionId) {
            log.warning(`Could not extract region ID from ${targetUrl}. Skipping.`);
            continue;
        }

        log.info(`Target: region ${regionMeta.regionId} (type ${regionMeta.regionType}), market=${regionMeta.market}`);
        let apiSucceeded = false;

        if (preferJson) {
            for (let page = 1; page <= maxPages && saved < resultsWanted; page += 1) {
                try {
                    const apiPayload = await fetchJsonApiPage({
                        target: regionMeta,
                        page,
                        pageSize,
                        proxyConfiguration: proxyConf,
                        timeoutMs: requestTimeoutMs,
                        maxRetries,
                    });
                    const homes = parseHomesFromApi(apiPayload);
                    if (!homes.length) break;
                    stats.methodsUsed.add('json-api');
                    stats.apiPages += 1;

                    const tasks = homes.map((listing) =>
                        limiter(async () => {
                            if (saved >= resultsWanted) return;

                            const id = listing.propertyId || listing.url;
                            if (id && seen.has(id)) return;
                            if (id) seen.add(id);

                            let detail = {};
                            if (collectDetails && listing.url) {
                                await delay(randBetween(delayMinMs, delayMaxMs));
                                detail = await fetchDetailPage({
                                    url: listing.url,
                                    proxyConfiguration: proxyConf,
                                    timeoutMs: requestTimeoutMs,
                                    maxRetries,
                                });
                            }

                            const property = buildProperty({ listing, detail, source: 'json-api' });
                            await Dataset.pushData(property);
                            saved += 1;
                        })
                    );

                    await Promise.all(tasks);

                    apiSucceeded = true;

                    if (saved >= resultsWanted) break;
                    await delay(randBetween(delayMinMs, delayMaxMs));
                } catch (err) {
                    stats.errors += 1;
                    log.warning(`JSON API page failed: ${err.message}`);
                    const statusInMsg = Number(err.message.match(/\d+/)?.[0]);
                    if (BLOCK_STATUS.has(statusInMsg)) break;
                }
            }
        }

        if (saved < resultsWanted && useHtmlFallback) {
            for (let page = 1; page <= maxPages && saved < resultsWanted; page += 1) {
                const pageUrl = page === 1 ? regionMeta.url : `${regionMeta.url}${regionMeta.url.includes('?') ? '&' : '?'}page=${page}`;
                try {
                    const html = await fetchSearchHtml({
                        url: pageUrl,
                        proxyConfiguration: proxyConf,
                        timeoutMs: requestTimeoutMs,
                        maxRetries,
                    });
                    const listings = parseHtmlListPage(html);
                    if (!listings.length) break;

                    stats.methodsUsed.add('html');
                    stats.htmlPages += 1;

                    for (const listing of listings) {
                        if (saved >= resultsWanted) break;
                        const id = listing.propertyId || listing.url;
                        if (id && seen.has(id)) continue;
                        if (id) seen.add(id);

                        let detail = {};
                        if (collectDetails && listing.url) {
                            await delay(randBetween(delayMinMs, delayMaxMs));
                            detail = await fetchDetailPage({
                                url: listing.url,
                                proxyConfiguration: proxyConf,
                                timeoutMs: requestTimeoutMs,
                                maxRetries,
                            });
                        }

                        const property = buildProperty({ listing, detail, source: 'html' });
                        await Dataset.pushData(property);
                        saved += 1;
                    }

                    if (saved >= resultsWanted) break;
                    await delay(randBetween(delayMinMs, delayMaxMs));
                } catch (err) {
                    stats.errors += 1;
                    log.warning(`HTML fallback failed for page ${page}: ${err.message}`);
                }
            }
        }

        if (saved < resultsWanted && usePlaywright) {
            const listings = await fetchViaPlaywright({ url: regionMeta.url, proxyConfiguration: proxyConf });
            if (listings.length) stats.methodsUsed.add('playwright');

            for (const listing of listings) {
                if (saved >= resultsWanted) break;
                const id = listing.propertyId || listing.url;
                if (id && seen.has(id)) continue;
                if (id) seen.add(id);

                let detail = {};
                if (collectDetails && listing.url) {
                    await delay(randBetween(delayMinMs, delayMaxMs));
                    detail = await fetchDetailPage({
                        url: listing.url,
                        proxyConfiguration: proxyConf,
                        timeoutMs: requestTimeoutMs,
                        maxRetries,
                    });
                }

                const property = buildProperty({ listing, detail, source: 'playwright' });
                await Dataset.pushData(property);
                saved += 1;
            }
        }

        if (!apiSucceeded && stats.htmlPages === 0 && saved === 0) {
            log.warning(`No results found for target ${regionMeta.url}.`);
        }
    }

    const totalTime = (Date.now() - startTime) / 1000;
    const methodsUsed = Array.from(stats.methodsUsed);

    log.info('='.repeat(70));
    log.info('FINAL STATISTICS');
    log.info('='.repeat(70));
    log.info(`Properties Saved: ${saved}/${resultsWanted}`);
    log.info(`API Pages: ${stats.apiPages}, HTML Pages: ${stats.htmlPages}`);
    log.info(`Errors: ${stats.errors}`);
    log.info(`Runtime: ${totalTime.toFixed(2)}s`);
    log.info(`Methods Used: ${methodsUsed.join(', ') || 'none'}`);
    log.info('='.repeat(70));

    if (saved === 0) {
        const errorMsg = 'Failed to scrape any properties. Check proxy, region ID, or Redfin availability.';
        log.error(errorMsg);
        await Actor.fail(errorMsg);
    } else {
        await Actor.setValue('OUTPUT_SUMMARY', {
            propertiesSaved: saved,
            runtimeSeconds: totalTime,
            methodsUsed,
        });
    }
} catch (error) {
    log.error(`CRITICAL ERROR: ${error.message}`);
    log.exception(error, 'Actor failed with exception');
    throw error;
} finally {
    await Actor.exit();
}
