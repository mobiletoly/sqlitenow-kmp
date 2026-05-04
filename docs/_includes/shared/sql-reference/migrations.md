# Migrations

Migration files move existing databases from one schema version to the next.
The `schema/` directory always contains the latest full schema, while
`migration/` contains incremental upgrade SQL for databases already installed
on a device.

{% if include.platform == "dart" %}
For the full Dart migration guide, see
[Flutter/Dart Migrations]({{ site.baseurl }}/flutter/migrations/).
{% elsif include.platform == "kmp" %}
For the full KMP migration guide, see
[KMP Migrations]({{ site.baseurl }}/kmp/migrations/).
{% endif %}
