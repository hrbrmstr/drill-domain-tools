# drill-domain-tools

A set of Apache Drill UDFs for working with Internet Domain Names

## UDFs

There is one UDF:

- `suffix_extract(domain-string)`: Given a valid internet domain name (FQDN or otherwise) this will return a map with fields for the `tld`, `assigned`, `subdomain`, and `hostname` for all those that are present. 

It relies on the [`crawler-commons`](https://github.com/crawler-commons/crawler-commons) Java library.

## Building

Retrieve the dependencies and build the UDF:

```
make deps
make udf
```

To automatically install it locally, ensure `DRILL_HOME` is set (the `Makefile` has a default of `/usr/local/drill`) and:

```
make install
```

Assuming you're running in standalone mode, you can then do:

```
make restart
```

You can manually copy:

- `target/drill-domain-tools-1.0.jar`
- `target/drill-domain-tools-1.0-sources.jar`
- `deps/crawler-commons-0.10.jar`

(after a successful build) to your `$DRILL_HOME/jars/3rdparty` directory and manually restart Drill as well.

## Example

Using the following query:

```
SELECT
  a.dom AS dom,
  a.rec.hostname AS host,
  a.rec.assigned AS assigned,
  a.rec.tld AS tld,
  a.rec.subdomain AS subdomain
FROM
  (SELECT dom, suffix_extract(dom) AS rec
  FROM
    (SELECT 'somehost.subnet.example.co.uk' AS dom
     FROM (VALUES((1))))) a;
```

Here's the output:

```
$ drill-conf
apache drill 1.14.0-SNAPSHOT
"this isn't your grandfather's sql"
0: jdbc:drill:> SELECT
. . . . . . . >   a.dom AS dom,
. . . . . . . >   a.rec.hostname AS host,
. . . . . . . >   a.rec.assigned AS assigned,
. . . . . . . >   a.rec.tld AS tld,
. . . . . . . >   a.rec.subdomain AS subdomain
. . . . . . . > FROM
. . . . . . . >   (SELECT dom, suffix_extract(dom) AS rec
. . . . . . . >   FROM
. . . . . . . >     (SELECT 'somehost.subnet.example.co.uk' AS dom
. . . . . . . >      FROM (VALUES((1))))) a;
+--------------------------------+-----------+----------------+--------+------------+
|              dom               |   host    |    assigned    |  tld   | subdomain  |
+--------------------------------+-----------+----------------+--------+------------+
| somehost.subnet.example.co.uk  | somehost  | example.co.uk  | co.uk  | subnet     |
+--------------------------------+-----------+----------------+--------+------------+
1 row selected (0.182 seconds)
```