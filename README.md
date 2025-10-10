# fluent-plugin-druid-query

[Fluentd](https://fluentd.org/) plugin to query druid.


## Installation

### RubyGems

```
$ gem install fluent-plugin-druid-query
```

### Bundler

Add following line to your Gemfile:

```ruby
gem "fluent-plugin-druid-query"
```

And then execute:

```
$ bundle
```


## plugin : druid_query

### behavior

### parameters

### example

```
<source>
  @type druid_query

  druid_url http://localhost:4567
  druid_user user
  druid_password password

  interval 5

  <query>
    sql select name, age from test
    subtag test
    cache false
    metadata id:query1
    generate_record true
    generate_info true
  </query>
</source>

<match druid_query**>
  @type stdout
</match>
```

## Copyright

* Copyright(c) 2025- Thomas Tych
* License
  * Apache License, Version 2.0
