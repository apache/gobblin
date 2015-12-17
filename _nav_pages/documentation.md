---
layout: page
title: Documentation
permalink: /doc/
order: 3
---

# Wiki

Please refer to our [Wiki](https://github.com/linkedin/gobblin/wiki) for the full documentation

# Papers and presentations

{% for post in site.categories.talks %}

# <span class="post-meta">{{ post.date | date: "%b %-d, %Y" }} - [{{ post.title }}]({{ site.baseurl }}/{{ post.url }})</span>

  {{ post.excerpt }} [(full post)]({{ post.url }})

{% endfor %}
