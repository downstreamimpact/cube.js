const PostgresQuery = require('./PostgresQuery');
const SmartPreAggregations = require('./SmartPreAggregations')


class SmartPostgresQuery extends PostgresQuery {
  newPreAggregations() {
    return new SmartPreAggregations(this, this.options.historyQueries || [], this.options.cubeLatticeCache);
  }
}

module.exports = SmartPostgresQuery;
