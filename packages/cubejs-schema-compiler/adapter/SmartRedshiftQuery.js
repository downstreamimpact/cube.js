const RedshiftQuery = require('./RedshiftQuery');
const SmartPreAggregations = require('./SmartPreAggregations')


class SmartRedshiftQuery extends RedshiftQuery {
  newPreAggregations() {
    return new SmartPreAggregations(this, this.options.historyQueries || [], this.options.cubeLatticeCache);
  }
}

module.exports = SmartRedshiftQuery;
