import sqlFormatter from './../src/sqlFormatter';
import behavesLikeSqlFormatter from './behavesLikeSqlFormatter';
import dedent from 'dedent-js';

describe('SparkSqlFormatter', () => {
  behavesLikeSqlFormatter('spark');

  const format = (query, cfg = {}) => sqlFormatter.format(query, { ...cfg, language: 'spark' });

  it('formats WINDOW specification as top level', () => {
    const result = format(
      'SELECT *, LAG(value) OVER wnd AS next_value FROM tbl WINDOW wnd as (PARTITION BY id ORDER BY time);'
    );
    expect(result).toBe(dedent/* sql */ `
      SELECT
        *,
        LAG(value) OVER wnd AS next_value
      FROM
        tbl
      WINDOW
        wnd as (
          PARTITION BY
            id
          ORDER BY
            time
        );
    `);
  });

  it('formats window function and end as inline', () => {
    const result = format(
      'SELECT window(time, "1 hour").start AS window_start, window(time, "1 hour").end AS window_end FROM tbl;'
    );
    expect(result).toBe(dedent/* sql */ `
      SELECT
        window(time, "1 hour").start AS window_start,
        window(time, "1 hour").end AS window_end
      FROM
        tbl;
    `);
  });

  it('does not add spaces around ${value} params', () => {
    const result = format('SELECT ${var_name};');
    expect(result).toBe(dedent/* sql */ `
      SELECT
        \${var_name};
    `);
  });

  it('replaces $variables and ${variables} with param values', () => {
    const result = format('SELECT $var1, ${var2};', {
      params: {
        var1: "'var one'",
        var2: "'var two'"
      }
    });
    expect(result).toBe(dedent/* sql */ `
      SELECT
        'var one',
        'var two';
    `);
  });
});
