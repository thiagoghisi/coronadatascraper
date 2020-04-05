import * as fetch from '../../lib/fetch/index.js';
import * as parse from '../../lib/parse.js';
import * as transform from '../../lib/transform.js';
import * as geography from '../../lib/geography/index.js';
import datetime from '../../lib/datetime/old/index.js';

import br_muni from 'country-levels/br_muni.json';

const scraper = {
  country: 'BRA',
  type: 'csv',
  priority: 1,
  url: 'https://brasil.io/dataset/covid19/caso?place_type=city&format=csv',
  timeseries: true,
  aggregate: 'county',

  _counties: Object.values(br_muni).map(c => c.countrylevel_id),

  async scraper() {
    const data = await fetch.csv(this.url, false);
    const scrapeDate = process.env.SCRAPE_DATE ? datetime.getYYYYMMDD(process.env.SCRAPE_DATE) : datetime.getYYYYMMDD();
    let latestDate = data[0].date;

    if (datetime.dateIsBefore(latestDate, scrapeDate)) {
      console.error('  🚨 Timeseries for BRA has not been updated, using %s instead of %s', latestDate, scrapeDate);
    } else {
      latestDate = datetime.getYYYYMMDD(scrapeDate);
    }

    let counties = data
      .filter(row => {
        return row.date === latestDate && row.city_ibge_code !== '';
      })
      .map(row => {
        return {
          cases: parse.number(row.confirmed),
          deaths: parse.number(row.deaths),
          county: `br_muni:${row.city_ibge_code}`,
          state: `iso2:BR-${row.state}`,
          aggregate: 'county'
        };
      });

    counties = geography.addEmptyRegions(counties, this._counties, 'county');
    counties.push(transform.sumData(counties));

    return counties;
  }
};

export default scraper;