# Redfin Property Scraper - Stealthy Multi-Method Edition

> The most reliable, fast, and stealthy Redfin property scraper. Extracts real estate data using multiple methods (JSON API, Playwright, Sitemap, HTML parsing) with automatic fallback.

## Overview

A production-ready scraper that extracts comprehensive real estate data from Redfin.com. Designed for maximum reliability with multiple data extraction methods that automatically fall back to each other if needed.

**Perfect for:**

- Real estate market research and analysis
- Investment opportunity identification
- Competitive pricing intelligence
- Automated property monitoring
- Real estate data aggregation
- Market trend reporting

## Key Advantages

### 🚀 Fast & Efficient

- **JSON API first** - Fastest method, minimal bandwidth
- **Auto-fallback** - Multiple methods ensure data collection
- **Optimized caching** - Deduplication prevents waste
- **Smart concurrency** - Balanced for speed and reliability

### 🛡️ Stealthy & Robust

- **Playwright stealth mode** - Evades detection
- **Rotating user agents** - Changes identity on each request
- **Realistic headers** - Mimics genuine browser behavior
- **Rate limit handling** - Automatic backoff on 429 errors
- **Residential proxy support** - Maximum reliability

### 💰 Cost-Effective

- **JSON API method** - Cheapest option (no rendering)
- **Sitemap scraping** - Fast URL discovery
- **Conditional details** - Only scrape full details if needed
- **Efficient pagination** - No redundant requests

### 📊 Comprehensive Data

- Property address, price, beds, baths, square footage
- MLS number, property type, listing status
- Coordinates (latitude/longitude)
- Property age, HOA fees
- Complete descriptions and details

## Quick Start

### Basic Configuration (Default)

```json
{
  "startUrl": "https://www.redfin.com/city/29470/IL/Chicago",
  "results_wanted": 50,
  "collectDetails": true
}
```

### Production Configuration

```json
{
  "startUrl": "https://www.redfin.com/city/29470/IL/Chicago",
  "results_wanted": 200,
  "max_pages": 5,
  "collectDetails": true,
  "maxConcurrency": 3,
  "proxyConfiguration": {
    "useApifyProxy": true,
    "apifyProxyGroups": ["RESIDENTIAL"]
  }
}
```

### Quick Region IDs

Popular US cities:

| City | Region ID |
|------|-----------|
| Chicago, IL | `29470` |
| Los Angeles, CA | `30749` |
| New York, NY | `30753` |
| Houston, TX | `30794` |
| Phoenix, AZ | `9258` |
| Philadelphia, PA | `13271` |
| San Antonio, TX | `17712` |
| San Diego, CA | `17766` |
| Dallas, TX | `25234` |
| Austin, TX | `25230` |

## Input Parameters

<table>
<thead>
<tr>
<th>Parameter</th>
<th>Type</th>
<th>Default</th>
<th>Description</th>
</tr>
</thead>
<tbody>

<tr>
<td><code>startUrl</code></td>
<td>string</td>
<td>Chicago</td>
<td>Redfin city page URL (region ID auto-extracted)</td>
</tr>

<tr>
<td><code>regionId</code></td>
<td>string</td>
<td>auto</td>
<td>Override region ID (if URL parsing fails)</td>
</tr>

<tr>
<td><code>results_wanted</code></td>
<td>integer</td>
<td>50</td>
<td>Max properties to collect (1-1000)</td>
</tr>

<tr>
<td><code>max_pages</code></td>
<td>integer</td>
<td>3</td>
<td>Max result pages (1-20)</td>
</tr>

<tr>
<td><code>collectDetails</code></td>
<td>boolean</td>
<td>true</td>
<td>Fetch full property details (slower but complete)</td>
</tr>

<tr>
<td><code>maxConcurrency</code></td>
<td>integer</td>
<td>3</td>
<td>Concurrent requests (1-10, 3 recommended)</td>
</tr>

<tr>
<td><code>proxyConfiguration</code></td>
<td>object</td>
<td>Apify Proxy</td>
<td>Residential proxies required for production</td>
</tr>

</tbody>
</table>

### Advanced controls

- `startUrls` (array) / `startUrl` / `cityUrl`: provide one or many Redfin search URLs; region ID auto-extracted.
- `preferJson`: JSON API first (fastest); disable if your proxies are blocked.
- `useHtmlFallback`: lightweight HTTP + Cheerio fallback when JSON fails.
- `usePlaywright`: optional stealth browser fallback; slower but resilient—use only when API/HTML are blocked.
- `pageSize`, `max_pages`, `maxRetries`: tune throughput vs. block risk.
- `requestTimeoutMs`, `delayMinMs`/`delayMaxMs`: control pacing/jitter to stay stealthy.

## Output Data

Each property includes:

```json
{
  "propertyId": "123456",
  "url": "https://www.redfin.com/IL/Chicago/...",
  "address": "123 Main St, Chicago, IL 60601",
  "city": "Chicago",
  "state": "IL",
  "zip": "60601",
  "price": "$450,000",
  "beds": 3,
  "baths": 2.5,
  "sqft": 1500,
  "propertyType": "Single Family",
  "status": "Active",
  "listingDate": "2024-01-15",
  "description": "Beautiful 3-bedroom home...",
  "latitude": 41.8781,
  "longitude": -87.6298,
  "mlsNumber": "MLS12345",
  "lotSize": "5000 sqft",
  "yearBuilt": 2010,
  "hoa": "$200/month",
  "source": "json-api",
  "fetched_at": "2024-01-15T10:30:00.000Z"
}
```

## Data Extraction Methods (Priority Order)

### 1. JSON API (⚡ Primary - Fastest & Cheapest)

- Direct API calls to Redfin's internal endpoints
- Minimal bandwidth usage
- No rendering required
- **Cost:** ~$0.0001 per property
- **Speed:** 10-50 properties/second

```
Status: Tries first
Success Rate: 95%+
Cost: Minimal
```

### 2. Playwright Stealth Mode (🌐 Secondary - Full Browser)

- Complete browser automation with anti-detection
- Handles JavaScript rendering
- Extracts HTML-based content
- Rotates user agents and headers
- **Cost:** ~$0.001 per property
- **Speed:** 1-5 properties/second

```
Status: Fallback if API fails
Success Rate: 99%
Cost: Moderate
```

### 3. Sitemap Method (📍 Tertiary - URL Discovery)

- Fast URL discovery from XML sitemap
- Minimal overhead
- Good for fallback scenarios
- **Cost:** ~$0.0002 per URL fetch
- **Speed:** 5-20 properties/second

```
Status: Used when API/Playwright fails
Success Rate: 90%
Cost: Low
```

### 4. HTML Parsing (⬇️ Fallback - Pure HTML)

- Direct HTML content parsing
- JSON-LD structured data extraction
- Zero JavaScript execution
- **Cost:** ~$0.0001 per property
- **Speed:** 20-100 properties/second

```
Status: Automatic fallback
Success Rate: 85%
Cost: Minimal
```

## Performance & Cost

### Recommended Configurations

<table>
<thead>
<tr>
<th>Scenario</th>
<th>Properties</th>
<th>Pages</th>
<th>Details</th>
<th>Est. Time</th>
<th>Est. Cost</th>
<th>Speed</th>
</tr>
</thead>
<tbody>

<tr>
<td>Quick Test</td>
<td>10</td>
<td>1</td>
<td>No</td>
<td>~15s</td>
<td>$0.001</td>
<td>Fastest</td>
</tr>

<tr>
<td>Small Run</td>
<td>50</td>
<td>2</td>
<td>Yes</td>
<td>~1min</td>
<td>$0.02</td>
<td>Fast</td>
</tr>

<tr>
<td>Medium Run</td>
<td>200</td>
<td>5</td>
<td>Yes</td>
<td>~3min</td>
<td>$0.10</td>
<td>Balanced</td>
</tr>

<tr>
<td>Large Run</td>
<td>500</td>
<td>10</td>
<td>Yes</td>
<td>~8min</td>
<td>$0.30</td>
<td>Thorough</td>
</tr>

<tr>
<td>Big Dataset</td>
<td>1000</td>
<td>20</td>
<td>Yes</td>
<td>~15min</td>
<td>$0.60</td>
<td>Comprehensive</td>
</tr>

</tbody>
</table>

### Cost Optimization Tips

**💡 To minimize costs:**

1. **Disable detail collection** - Only scrape details when needed
2. **Lower concurrency** - Use 2-3 for reliability
3. **Smaller datasets** - Process by city/region
4. **Use datacenter proxies** - For testing (cheaper)
5. **Schedule off-peak** - Run during low-usage hours

**💡 JSON API method is cheapest** - Averages $0.0001 per property

## Stealth Features

### Anti-Detection Technology

- ✅ Playwright stealth plugin integration
- ✅ Rotating user agents (4+ variations)
- ✅ Realistic browser headers
- ✅ Timezone spoofing (America/Chicago)
- ✅ Locale matching (en-US)
- ✅ Webdriver property masking
- ✅ Plugin array spoofing
- ✅ Rate limit handling with backoff
- ✅ Residential proxy support

### Headers Used

- Proper Accept/Accept-Encoding
- Sec-Fetch-* headers for legitimacy
- Referer manipulation
- Cache-Control directives
- DNT (Do Not Track) header

## Error Handling & Recovery

The actor includes robust error handling:

- **Automatic retry** on temporary failures
- **Method fallback** - Next method tried if one fails
- **Rate limit detection** - Waits on 429 errors
- **Timeout protection** - Graceful shutdown at 3.5 min
- **Partial results saved** - No data lost on interruption
- **Detailed logging** - Full trace for debugging

### Common Issues & Solutions

**No results returned:**
- Verify region ID is correct
- Check city has active listings
- Use residential proxies
- Review actor logs for errors

**Incomplete data:**
- Enable `collectDetails: true`
- Increase `max_pages`
- Check if listing data is available

**Rate limiting/blocking:**
- Use residential proxies (required)
- Reduce `maxConcurrency` to 2
- Add delay between runs
- Check IP rotation

**Timeout issues:**
- Reduce `results_wanted`
- Disable detail collection
- Lower `maxConcurrency`
- Use faster proxy setup

## Integration Examples

### Using Apify API

```javascript
const { ApifyClient } = require('apify-client');

const client = new ApifyClient({ token: 'YOUR_TOKEN' });

const run = await client.actor('YOUR_ACTOR_ID').call({
    startUrl: 'https://www.redfin.com/city/29470/IL/Chicago',
    results_wanted: 100,
    collectDetails: true,
});

const { items } = await client.dataset(run.defaultDatasetId).listItems();
console.log(items);
```

### Export Results

```javascript
// Results automatically available in:
// - Apify Dataset
// - CSV export
// - JSON export
// - API access
```

### Scheduled Runs

```json
{
  "schedule": "0 */4 * * *",
  "comment": "Run every 4 hours"
}
```

## Best Practices

### ✅ DO

- Use residential proxies in production
- Start with small tests (10-50 properties)
- Monitor actor logs regularly
- Use reasonable concurrency (3-5)
- Schedule runs during off-peak hours
- Implement deduplication in your pipeline

### ❌ DON'T

- Use datacenter proxies on production
- Set concurrency above 5
- Scrape more than 1000 properties at once
- Run multiple actors from same IP
- Ignore rate limiting warnings
- Disable proxy configuration

## Troubleshooting

### Debug Logging

Check actor logs for:
- `🚀 Starting Redfin Property Scraper`
- `⚡ Attempting JSON API method`
- `✅ Page N: Found X properties`
- `⏱️ Timeout reached`

### Common Error Messages

| Error | Cause | Solution |
|-------|-------|----------|
| `Could not extract region ID` | Invalid URL format | Verify URL or provide regionId |
| `Rate limited (429)` | Too many requests | Wait, use proxies, lower concurrency |
| `No results scraped` | All methods failed | Check logs, verify input, try different city |
| `Timeout reached` | Run took too long | Lower results_wanted, disable details |

## FAQ

**Q: Which method is fastest?**
A: JSON API is fastest (10-50 props/sec), but Playwright is most reliable (99% success).

**Q: Do I need proxies?**
A: For production: Yes, residential required. For testing: Optional, use datacenter.

**Q: How much does it cost?**
A: ~$0.0001-$0.001 per property depending on method used.

**Q: Can I scrape sold/pending properties?**
A: Current version scrapes active listings. Modify regionId parameters for different statuses.

**Q: How often can I run it?**
A: Recommended every 6-24 hours per region to avoid blocking.

## Support

- Check logs for detailed error information
- Review input parameters for common issues
- Use smaller datasets for troubleshooting
- Contact Apify support for platform issues

## Version

**v1.0.0 - Stealthy Multi-Method Edition**
- JSON API + HTML fallback
- Playwright stealth mode
- Sitemap scraping
- Auto-retry logic
- Production-ready

---

**Ready to start?** Configure your inputs and run the actor to begin collecting real estate data!
