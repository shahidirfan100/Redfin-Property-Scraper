// Redfin Property Scraper - Stealthy Playwright + Multi-Method Extraction
import { Actor, log } from 'apify';
import { Dataset, PlaywrightCrawler, gotScraping } from 'crawlee';
import { chromium } from 'playwright';
import { load as cheerioLoad } from 'cheerio';

// ============================================================================
// CONSTANTS & CONFIGURATION
// ============================================================================

const REDFIN_BASE = 'https://www.redfin.com';
const REDFIN_API_GIS = `${REDFIN_BASE}/stingray/api/gis`;
const REDFIN_SITEMAP = `${REDFIN_BASE}/sitemap_homes.xml`;

// Stealthy User Agents rotation
const USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
];

const STEALTHY_HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'en-US,en;q=0.9',
    'DNT': '1',
    'Referer': REDFIN_BASE,
    'Sec-Ch-Ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
    'Sec-Ch-Ua-Mobile': '?0',
    'Sec-Ch-Ua-Platform': '"Windows"',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'none',
    'Sec-Fetch-User': '?1',
    'Upgrade-Insecure-Requests': '1',
    'Cache-Control': 'max-age=0',
};

const API_HEADERS = {
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'en-US,en;q=0.9',
    'Content-Type': 'application/json;charset=UTF-8',
    'X-Requested-With': 'XMLHttpRequest',
};

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

const getRandomUserAgent = () => USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)];

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const cleanText = (text) => {
    if (!text) return null;
    return text.replace(/\s+/g, ' ').trim();
};

const cleanHtml = (html) => {
    if (!html) return null;
    const $ = cheerioLoad(html);
    $('script, style, noscript, meta, link').remove();
    return $.root().text().replace(/\s+/g, ' ').trim();
};

const ensureAbsoluteUrl = (url) => {
    if (!url) return null;
    if (url.startsWith('http')) return url;
    return `${REDFIN_BASE}${url.startsWith('/') ? '' : '/'}${url}`;
};

const extractByLabel = ($, labels) => {
    const normalized = labels.map((l) => l.toLowerCase());

    const dlRows = $('dl').find('dt, dd');
    for (let i = 0; i < dlRows.length; i += 2) {
        const label = cleanText($(dlRows[i]).text())?.toLowerCase();
        const value = cleanText($(dlRows[i + 1]).text());
        if (label && normalized.some((l) => label.includes(l))) return value;
    }

    const tableRows = $('tr');
    for (const row of tableRows) {
        const cells = $(row).find('td, th');
        if (cells.length < 2) continue;
        const label = cleanText($(cells[0]).text())?.toLowerCase();
        const value = cleanText($(cells[1]).text());
        if (label && normalized.some((l) => label.includes(l))) return value;
    }

    const listItems = $('li');
    for (const li of listItems) {
        const text = cleanText($(li).text());
        if (!text) continue;
        const lower = text.toLowerCase();
        const match = normalized.find((l) => lower.startsWith(`${l}:`) || lower.includes(`${l} `));
        if (match) {
            return cleanText(text.split(':').slice(1).join(':')) || text;
        }
    }
    return null;
};

const extractRegionIdFromUrl = (url) => {
    try {
        const match = url.match(/\/city\/(\d+)\//);
        return match ? match[1] : null;
    } catch {
        return null;
    }
};

const createLimiter = (maxConcurrency) => {
    let active = 0;
    const queue = [];
    const next = () => {
        if (active >= maxConcurrency || queue.length === 0) return;
        active += 1;
        const { task, resolve, reject } = queue.shift();
        task()
            .then((res) => {
                resolve(res);
            })
            .catch((err) => {
                reject(err);
            })
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

// ============================================================================
// PLAYWRIGHT BROWSER SETUP - STEALTHY MODE
// ============================================================================

const createStealthyBrowser = async (proxyConfiguration) => {
    const launchOptions = {
        headless: true,
        args: [
            '--disable-blink-features=AutomationControlled',
            '--disable-dev-shm-usage',
            '--disable-gpu',
            '--no-first-run',
            '--no-default-browser-check',
            '--disable-popup-blocking',
            '--disable-prompt-on-repost',
            '--disable-background-timer-throttling',
            '--disable-renderer-backgrounding',
            '--disable-backgrounding-occluded-windows',
            '--disable-breakpad',
            '--disable-client-side-phishing-detection',
            '--disable-component-extensions-with-background-pages',
            '--disable-component-extensions-with-native-dialogs',
            '--disable-extensions-except',
            '--disable-features=InterestFeedContentSuggestions',
            '--disable-sync',
            '--metrics-recording-only',
            '--mute-audio',
            '--no-service-autorun',
        ],
    };

    if (proxyConfiguration) {
        const proxyUrl = await proxyConfiguration.newUrl();
        launchOptions.proxy = { server: proxyUrl };
    }

    return chromium.launch(launchOptions);
};

// ============================================================================
// JSON API METHOD - PRIMARY
// ============================================================================

const fetchViaJsonAPI = async ({ regionId, page = 1, proxyConfiguration }) => {
    const params = new URLSearchParams({
        al: 1,
        market: 'chicago',
        num_homes: 350,
        page_number: page,
        region_id: regionId,
        region_type: 6,
        status: 9,
        uipt: '1,2,3,4,5,6,7,8',
        v: 8,
    });

    const url = `${REDFIN_API_GIS}?${params.toString()}`;

    try {
        const res = await gotScraping({
            url,
            responseType: 'text',
            headers: {
                ...API_HEADERS,
                'User-Agent': getRandomUserAgent(),
                'Referer': REDFIN_BASE,
            },
            proxyUrl: proxyConfiguration ? await proxyConfiguration.newUrl() : undefined,
            timeout: { request: 30000 },
            throwHttpErrors: false,
            retry: { limit: 2 },
        });

        if (res.statusCode === 429) {
            log.warning('⚠️ Rate limited (429). Implementing backoff...');
            await sleep(5000 + Math.random() * 5000);
            return null;
        }

        if (res.statusCode !== 200) {
            log.warning(`❌ API Error ${res.statusCode}: ${res.body?.substring(0, 200)}`);
            return null;
        }

        const cleanJson = res.body.replace(/^{}&&/, '').trim();
        return JSON.parse(cleanJson);
    } catch (err) {
        log.warning(`⚠️ JSON API Error: ${err.message}`);
        return null;
    }
};

// ============================================================================
// PLAYWRIGHT METHOD - STEALTHY BROWSER AUTOMATION
// ============================================================================

const fetchViaPlaywright = async ({ url, proxyConfiguration, collectDetails = false }) => {
    let browser = null;
    let context = null;
    let page = null;

    try {
        browser = await createStealthyBrowser(proxyConfiguration);
        context = await browser.newContext({
            userAgent: getRandomUserAgent(),
            viewport: { width: 1920, height: 1080 },
            ignoreHTTPSErrors: true,
            extraHTTPHeaders: STEALTHY_HEADERS,
            timezoneId: 'America/Chicago',
            locale: 'en-US',
        });

        // Add stealth scripts
        await context.addInitScript(() => {
            Object.defineProperty(navigator, 'webdriver', {
                get: () => false,
            });
            Object.defineProperty(navigator, 'plugins', {
                get: () => [1, 2, 3, 4, 5],
            });
        });

        page = await context.newPage();
        page.setDefaultTimeout(30000);
        page.setDefaultNavigationTimeout(30000);

        log.info(`🌐 Loading page: ${url}`);
        await page.goto(url, { waitUntil: 'networkidle' });

        // Wait for content to load
        await page.waitForTimeout(2000);

        const content = await page.content();
        const { properties, jsonLd } = await extractFromPageContent(content);

        return { properties, jsonLd, source: 'playwright' };
    } catch (err) {
        log.warning(`⚠️ Playwright Error: ${err.message}`);
        return null;
    } finally {
        if (page) await page.close().catch(() => {});
        if (context) await context.close().catch(() => {});
        if (browser) await browser.close().catch(() => {});
    }
};

// ============================================================================
// JSON-LD EXTRACTION
// ============================================================================

const extractJsonLd = (html) => {
    const $ = cheerioLoad(html);
    const jsonLdData = [];

    $('script[type="application/ld+json"]').each((_, el) => {
        try {
            const json = JSON.parse($(el).contents().text().trim());
            if (Array.isArray(json)) {
                jsonLdData.push(...json);
            } else {
                jsonLdData.push(json);
            }
        } catch {
            // Skip malformed JSON-LD
        }
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

// ============================================================================
// HTML PARSING METHOD - FALLBACK
// ============================================================================

const extractFromPageContent = async (html) => {
    const $ = cheerioLoad(html);
    const jsonLdArray = extractJsonLd(html);
    const propertyData = parsePropertyFromJsonLd(jsonLdArray);

    // Extract property listings from page
    const properties = [];
    $('[data-property-id]').each((_, el) => {
        const $el = $(el);
        const prop = {
            propertyId: $el.attr('data-property-id'),
            url: $el.find('a').attr('href'),
            address: $el.find('[data-address]').text().trim() || null,
            price: $el.find('[data-price]').text().trim() || null,
        };

        if (prop.propertyId || prop.url) {
            properties.push(prop);
        }
    });

    return { properties, jsonLd: propertyData };
};

const parseHtmlDetail = async (html) => {
    const $ = cheerioLoad(html);
    const jsonLdArray = extractJsonLd(html);
    const propertyData = parsePropertyFromJsonLd(jsonLdArray) || {};

    const metaDescription = $('meta[name="description"]').attr('content');
    const listingDateLabel = extractByLabel($, ['listed on', 'time on redfin', 'listed']);
    const lotSizeLabel = extractByLabel($, ['lot size', 'lot sqft']);
    const statusLabel = extractByLabel($, ['status']);
    const sqftLabel = extractByLabel($, ['square feet', 'sq ft', 'sqft']);

    return {
        title: propertyData.title || cleanText($('h1').first().text()) || null,
        price: propertyData.price || cleanText($('.price-info, [data-rf-test-id="abp-price"], .statsValue').first().text()) || null,
        beds: propertyData.beds || cleanText($('[data-beds], [data-rf-test-id="abp-beds"]').first().text()) || null,
        baths: propertyData.baths || cleanText($('[data-baths], [data-rf-test-id="abp-baths"]').first().text()) || null,
        sqft: propertyData.sqft || sqftLabel || cleanText($('[data-sqft], [data-rf-test-id="abp-sqft"]').first().text()) || null,
        address: propertyData.address || cleanText($('.address, [data-rf-test-id="abp-streetLine"]').first().text()) || null,
        city: propertyData.city || null,
        state: propertyData.state || null,
        zip: propertyData.zip || null,
        description:
            propertyData.description ||
            cleanText(metaDescription) ||
            cleanText($('.property-description, .remarks, [data-rf-test-id="abp-description"]').text()) ||
            null,
        latitude: propertyData.latitude || null,
        longitude: propertyData.longitude || null,
        lotSize: propertyData.lotSize || lotSizeLabel || null,
        yearBuilt: propertyData.yearBuilt || extractByLabel($, ['year built']) || null,
        hoa: propertyData.hoa || extractByLabel($, ['hoa dues', 'hoa fee']) || null,
        status: propertyData.status || statusLabel || cleanText($('[data-rf-test-id="abp-status"]').first().text()) || null,
        listingDate: propertyData.listingDate || listingDateLabel || null,
        mlsNumber: propertyData.mlsNumber || extractByLabel($, ['mls#', 'mls number']) || null,
    };
};

// ============================================================================
// SITEMAP PARSING METHOD
// ============================================================================

const fetchSitemapUrls = async ({ regionId, limit = 100, proxyConfiguration }) => {
    try {
        const sitemapRes = await gotScraping({
            url: REDFIN_SITEMAP,
            responseType: 'text',
            headers: { 'User-Agent': getRandomUserAgent() },
            proxyUrl: proxyConfiguration ? await proxyConfiguration.newUrl() : undefined,
            timeout: { request: 30000 },
            throwHttpErrors: false,
        });

        if (sitemapRes.statusCode !== 200) return [];

        const $ = cheerioLoad(sitemapRes.body);
        const urls = [];

        $('loc').each((_, el) => {
            const url = $(el).text().trim();
            if (url.includes('/home/') && urls.length < limit) {
                urls.push(url);
            }
        });

        log.info(`📍 Found ${urls.length} property URLs from sitemap`);
        return urls;
    } catch (err) {
        log.warning(`⚠️ Sitemap fetch error: ${err.message}`);
        return [];
    }
};

// ============================================================================
// PROPERTY BUILDER
// ============================================================================

const buildProperty = ({ listing, detail, source }) => {
    const propertyId = listing?.propertyId || listing?.mlsId?.value || detail?.id;
    const url = ensureAbsoluteUrl(listing?.url || detail?.url || (propertyId ? `${REDFIN_BASE}/home/${propertyId}` : null));

    const price = detail?.price || listing?.price || listing?.priceInfo?.amount;
    const beds = detail?.beds || listing?.beds;
    const baths = detail?.baths || listing?.baths;
    const sqft = detail?.sqft || listing?.sqFt;
    const address = detail?.address || listing?.streetLine?.value || listing?.address;
    const city = detail?.city || listing?.city;
    const state = detail?.state || listing?.state;
    const zip = detail?.zip || listing?.zip;

    const fullAddress =
        address && city && state ? `${address}, ${city}, ${state}${zip ? ` ${zip}` : ''}` : address || null;

    return {
        propertyId: propertyId || url,
        url,
        address: fullAddress,
        streetAddress: address,
        city,
        state,
        zip,
        price: price ? (typeof price === 'number' ? `$${price.toLocaleString()}` : price) : null,
        beds: beds ? parseInt(beds) : null,
        baths: baths ? parseFloat(baths) : null,
        sqft: sqft ? parseInt(sqft) : null,
        propertyType: listing?.propertyType || detail?.propertyType || null,
        status: detail?.status || listing?.status || listing?.mlsStatus?.value || null,
        listingDate: detail?.listingDate || listing?.listingDate || null,
        description: detail?.description || null,
        latitude: detail?.latitude || listing?.latLong?.latitude || listing?.lat || null,
        longitude: detail?.longitude || listing?.latLong?.longitude || listing?.lng || null,
        mlsNumber: detail?.mlsNumber || listing?.mlsNumber || listing?.mlsId?.value || null,
        lotSize: detail?.lotSize || listing?.lotSize?.amount || null,
        yearBuilt: detail?.yearBuilt || listing?.yearBuilt || null,
        hoa: detail?.hoa || listing?.hoa || listing?.hoaFee || null,
        source,
        fetched_at: new Date().toISOString(),
    };
};

// ============================================================================
// MAIN ACTOR LOGIC
// ============================================================================

await Actor.init();

try {
    const input = (await Actor.getInput()) || {};
    const {
        startUrl = 'https://www.redfin.com/city/29470/IL/Chicago',
        cityUrl,
        regionId: inputRegionId,
        collectDetails = true,
        results_wanted: resultsWantedRaw = 50,
        max_pages: maxPagesRaw = 3,
        maxConcurrency = 3,
        proxyConfiguration,
    } = input;

    const resultsWanted = Math.max(1, Number.isFinite(+resultsWantedRaw) ? +resultsWantedRaw : 1);
    const maxPages = Math.max(1, Number.isFinite(+maxPagesRaw) ? +maxPagesRaw : 1);
    const proxyConf = proxyConfiguration ? await Actor.createProxyConfiguration({ ...proxyConfiguration }) : undefined;

    const targetUrl = cityUrl || startUrl;
    let targetRegionId = inputRegionId || extractRegionIdFromUrl(targetUrl);

    if (!targetRegionId) {
        throw new Error('Could not extract region ID. Please provide a valid Redfin city URL.');
    }

    log.info('🚀 Starting Redfin Property Scraper - Stealthy Mode');
    log.info(`🏠 Target Region: ${targetRegionId}`);
    log.info(`📊 Target: ${resultsWanted} properties, max ${maxPages} pages`);
    log.info('📋 Method Priority: JSON API → Playwright HTML → Sitemap → Fallback');

    const seenIds = new Set();
    const limiter = createLimiter(Math.max(1, Math.min(10, maxConcurrency)));
    let saved = 0;
    let methodFailed = {};

    const startTime = Date.now();
    const MAX_RUNTIME_MS = 3.5 * 60 * 1000;
    const stats = { pagesProcessed: 0, propertiesSaved: 0, apiCalls: 0, errors: 0, methodsUsed: [] };

    // ========================================================================
    // ATTEMPT 1: JSON API METHOD (FASTEST & CHEAPEST)
    // ========================================================================

    log.info('⚡ Attempting JSON API method...');

    for (let page = 1; page <= maxPages && saved < resultsWanted; page += 1) {
        if (Date.now() - startTime > MAX_RUNTIME_MS) {
            log.info(`⏱️ Timeout reached. Gracefully stopping.`);
            await Actor.setValue('TIMEOUT_REACHED', true);
            break;
        }

        try {
            stats.apiCalls += 1;
            const apiData = await fetchViaJsonAPI({ regionId: targetRegionId, page, proxyConfiguration: proxyConf });

            if (!apiData || !apiData.payload?.homes) {
                methodFailed.api = true;
                log.warning('❌ JSON API method failed, moving to next method...');
                break;
            }

            const homes = apiData.payload.homes || [];
            log.info(`✅ Page ${page}: Found ${homes.length} properties via API`);

            if (!homes.length) break;

            const detailPromises = homes.map((listing) =>
                limiter(async () => {
                    if (saved >= resultsWanted) return;
                    const id = listing.propertyId || listing.mlsId?.value;
                    if (id && seenIds.has(id)) return;
                    if (id) seenIds.add(id);

                    try {
                        let detail = null;

                        if (collectDetails && listing.url) {
                            const detailUrl = `${REDFIN_BASE}${listing.url}`;
                            const detailRes = await gotScraping({
                                url: detailUrl,
                                headers: {
                                    ...STEALTHY_HEADERS,
                                    'User-Agent': getRandomUserAgent(),
                                },
                                responseType: 'text',
                                proxyUrl: proxyConf ? await proxyConf.newUrl() : undefined,
                                timeout: { request: 30000 },
                                throwHttpErrors: false,
                                retry: { limit: 1 },
                            });

                            if (detailRes.statusCode === 200) {
                                detail = await parseHtmlDetail(detailRes.body);
                            }
                        }

                        const property = buildProperty({ listing, detail, source: 'json-api' });
                        await Dataset.pushData(property);
                        saved += 1;
                        stats.propertiesSaved = saved;
                    } catch (err) {
                        stats.errors += 1;
                        log.warning(`⚠️ Error processing property: ${err.message}`);
                    }
                })
            );

            await Promise.all(detailPromises);
            stats.pagesProcessed = page;

            if (saved > 0 && page === 1) {
                log.info(`✅ JSON API: First page success! ${saved} properties saved.`);
                stats.methodsUsed.push('json-api');
            }
        } catch (err) {
            stats.errors += 1;
            methodFailed.api = true;
            log.warning(`⚠️ JSON API failed: ${err.message}`);
            break;
        }
    }

    // ========================================================================
    // ATTEMPT 2: PLAYWRIGHT METHOD (IF JSON API FAILED)
    // ========================================================================

    if (saved < resultsWanted && methodFailed.api) {
        log.info('🌐 Attempting Playwright (stealthy browser) method...');

        try {
            const playwrightResult = await fetchViaPlaywright({
                url: targetUrl,
                proxyConfiguration: proxyConf,
                collectDetails: true,
            });

            if (playwrightResult?.properties) {
                for (const listing of playwrightResult.properties) {
                    if (saved >= resultsWanted) break;

                    const id = listing.propertyId || listing.url;
                    if (id && seenIds.has(id)) continue;
                    if (id) seenIds.add(id);

                    try {
                        const property = buildProperty({
                            listing,
                            detail: null,
                            source: 'playwright',
                        });
                        await Dataset.pushData(property);
                        saved += 1;
                        stats.propertiesSaved = saved;
                    } catch (err) {
                        stats.errors += 1;
                    }
                }

                if (saved > 0) {
                    log.info(`✅ Playwright: ${saved} properties saved!`);
                    stats.methodsUsed.push('playwright');
                }
            }
        } catch (err) {
            log.warning(`⚠️ Playwright method failed: ${err.message}`);
        }
    }

    // ========================================================================
    // ATTEMPT 3: SITEMAP METHOD (FAST FALLBACK)
    // ========================================================================

    if (saved < resultsWanted) {
        log.info('📍 Attempting Sitemap method...');

        try {
            const sitemapUrls = await fetchSitemapUrls({
                regionId: targetRegionId,
                limit: resultsWanted - saved,
                proxyConfiguration: proxyConf,
            });

            for (const sitemapUrl of sitemapUrls) {
                if (saved >= resultsWanted) break;
                if (seenIds.has(sitemapUrl)) continue;
                seenIds.add(sitemapUrl);

                try {
                    const detailRes = await gotScraping({
                        url: sitemapUrl,
                        headers: {
                            ...STEALTHY_HEADERS,
                            'User-Agent': getRandomUserAgent(),
                        },
                        responseType: 'text',
                        proxyUrl: proxyConf ? await proxyConf.newUrl() : undefined,
                        timeout: { request: 30000 },
                        throwHttpErrors: false,
                        retry: { limit: 1 },
                    });

                    if (detailRes.statusCode === 200) {
                        const detail = await parseHtmlDetail(detailRes.body);
                        const property = buildProperty({
                            listing: { url: sitemapUrl },
                            detail,
                            source: 'sitemap',
                        });
                        await Dataset.pushData(property);
                        saved += 1;
                        stats.propertiesSaved = saved;

                        if (saved % 10 === 0) {
                            log.info(`✅ Sitemap: ${saved} properties collected...`);
                        }
                    }
                } catch (err) {
                    stats.errors += 1;
                }
            }

            if (stats.methodsUsed.length === 0 && saved > 0) {
                stats.methodsUsed.push('sitemap');
            }
        } catch (err) {
            log.warning(`⚠️ Sitemap method error: ${err.message}`);
        }
    }

    const totalTime = (Date.now() - startTime) / 1000;

    log.info('='.repeat(70));
    log.info('📊 FINAL STATISTICS');
    log.info('='.repeat(70));
    log.info(`✅ Properties Saved: ${saved}/${resultsWanted}`);
    log.info(`📄 Pages Processed: ${stats.pagesProcessed}/${maxPages}`);
    log.info(`🌐 API Calls: ${stats.apiCalls}`);
    log.info(`⚠️  Errors: ${stats.errors}`);
    log.info(`⏱️  Total Runtime: ${totalTime.toFixed(2)}s`);
    log.info(`⚡ Performance: ${(saved / totalTime).toFixed(2)} properties/second`);
    log.info(`🔧 Methods Used: ${stats.methodsUsed.join(', ') || 'None successful'}`);
    log.info('='.repeat(70));

    if (saved === 0) {
        const errorMsg = 'Failed to scrape any properties. Check logs and configuration.';
        log.error(`❌ ${errorMsg}`);
        await Actor.fail(errorMsg);
    } else {
        log.info(`✅ SUCCESS: Scraped ${saved} properties!`);
        await Actor.setValue('OUTPUT_SUMMARY', {
            propertiesSaved: saved,
            pagesProcessed: stats.pagesProcessed,
            runtime: totalTime,
            methodsUsed: stats.methodsUsed,
            success: true,
        });
    }
} catch (error) {
    log.error(`❌ CRITICAL ERROR: ${error.message}`);
    log.exception(error, 'Actor failed with exception');
    throw error;
} finally {
    await Actor.exit();
}
