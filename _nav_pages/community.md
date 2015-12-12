---
layout: page
title: Community
permalink: /community/
order: 5
---

# User group

Please [join us](https://groups.google.com/forum/#!forum/gobblin-users) at our [gobblin-users](mailto:gobblin-users@googlegroups.com) Google group for announcements, discussions, feedback and more.

# Blog posts

{% for post in site.categories.blog %}

# <span class="post-meta">{{ post.date | date: "%b %-d, %Y" }} - [{{ post.title }}]({{ post.url }})</span>

{% endfor %}


# Meetups

{% for post in site.categories.meetup %}

# <span class="post-meta">{{ post.date | date: "%b %-d, %Y" }} - [{{ post.title }}]({{ post.url }})</span>

{% endfor %}