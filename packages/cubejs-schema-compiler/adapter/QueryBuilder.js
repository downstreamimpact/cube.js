const postgres = require('./PostgresQuery');
const mysql = require('./MysqlQuery');
const mongobi = require('./MongoBiQuery');
const mssql = require('./MssqlQuery');
const bigquery = require('./BigqueryQuery');
const redshift = require('./RedshiftQuery');
const prestodb = require('./PrestodbQuery');
const vertica = require('./VerticaQuery');
const snowflake = require('./SnowflakeQuery');
const clickhouse = require('./ClickHouseQuery');
const hive = require('./HiveQuery');
const oracle = require('./OracleQuery');
const smart_postgres = require('./SmartPostgresQuery');
const smart_redshift = require('./SmartRedshiftQuery');

const ADAPTERS = {
  postgres,
  redshift,
  mysql,
  mongobi,
  mssql,
  bigquery,
  prestodb,
  qubole_prestodb: prestodb,
  athena: prestodb,
  vertica,
  snowflake,
  clickhouse,
  hive,
  oracle,
  smart_postgres,
  smart_redshift
};
exports.query = (compilers, dbType, queryOptions) => {
  if (!ADAPTERS[dbType]) {
    return null;
  }

  return new (ADAPTERS[dbType])(compilers, {
    ...queryOptions,
    externalQueryClass: queryOptions.externalDbType && ADAPTERS[queryOptions.externalDbType]
  });
};
