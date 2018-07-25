# drill-domain-tools

A set of Apache Drill UDFs for working with Internet Domain Names

## UDFs

There is one UDF:

- `suffix_extract(domain-string)`: Given a valid internet domain name (FQDN or otherwise) this will return a map with fields for the `public_suffix`, `tld`, `domain`, `top_private_domain` and `hostname` for all those that are present. 

This uses the _ancient_ `guava` v18 JAR that comes with Apache Drill, so there are no other 3rd party JARs to install.

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

(after a successful build) to your `$DRILL_HOME/jars/3rdparty` directory and manually restart Drill as well.

## Example

Using the following query:

```
SELECT
  a.dom AS dom,
  a.rec.hostname AS host,
  a.rec.domain AS dom,
  a.rec.public_suffix AS suf,
  a.rec.tld AS tld,
  a.rec.top_private_domain tpf
FROM
  (SELECT dom, suffix_extract(dom) AS rec
  FROM
    (SELECT 'somehost.example.com' AS dom
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
. . . . . . . >   a.rec.domain AS dom,
. . . . . . . >   a.rec.public_suffix AS suf,
. . . . . . . >   a.rec.tld AS tld,
. . . . . . . >   a.rec.top_private_domain tpf
. . . . . . . > FROM
. . . . . . . >   (SELECT dom, suffix_extract(dom) AS rec
. . . . . . . >   FROM
. . . . . . . >     (SELECT 'somehost.example.com' AS dom
. . . . . . . >      FROM (VALUES((1))))) a;
+-----------------------+-----------+----------+------+------+--------------+
|          dom          |   host    |   dom0   | suf  | tld  |     tpf      |
+-----------------------+-----------+----------+------+------+--------------+
| somehost.example.com  | somehost  | example  | com  | com  | example.com  |
+-----------------------+-----------+----------+------+------+--------------+
1 row selected (0.137 seconds)
```