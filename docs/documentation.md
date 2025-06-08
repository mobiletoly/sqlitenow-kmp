---
layout: page
title: Documentation
permalink: /documentation/
---

# Documentation

Comprehensive documentation for SQLiteNow, covering everything from basic setup to advanced features.

## Getting Started

If you're new to SQLiteNow, start with our [Getting Started]({{ site.baseurl }}/getting-started/) guide to set up your first project.

## Core Concepts

### Schema Definition
Learn how to define your database schema using SQL files with SQLiteNow annotations.

[Create Schema →]({{ site.baseurl }}/documentation/create-schema/)

### Querying Data
Learn how to write SELECT queries and retrieve data from your database.

[Query Data →]({{ site.baseurl }}/documentation/query-data/)

### Managing Data
Understand how to INSERT, UPDATE, and DELETE data in your database.

[Manage Data →]({{ site.baseurl }}/documentation/manage-data/)

## Quick Navigation

<div class="doc-nav-grid">
  <a href="{{ site.baseurl }}/documentation/create-schema/" class="doc-nav-card">
    <h3>📋 Create Schema</h3>
    <p>Define tables, indexes, and database structure with SQL and annotations</p>
  </a>
  
  <a href="{{ site.baseurl }}/documentation/query-data/" class="doc-nav-card">
    <h3>🔍 Query Data</h3>
    <p>Write SELECT queries to retrieve data as lists, single items, or reactive flows</p>
  </a>

  <a href="{{ site.baseurl }}/documentation/manage-data/" class="doc-nav-card">
    <h3>✏️ Manage Data</h3>
    <p>INSERT, UPDATE, and DELETE operations to modify your database</p>
  </a>
</div>

## Advanced Topics

More documentation sections will be added here as the library grows:

- **Migrations** - Managing database schema changes
- **Type Adapters** - Custom type conversions
- **Reactive Queries** - Real-time data with Flow
- **Testing** - Unit testing your database code

<style>
.doc-nav-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
  gap: 20px;
  margin: 30px 0;
}

.doc-nav-card {
  border: 1px solid #d0d7de;
  border-radius: 8px;
  padding: 20px;
  text-decoration: none;
  color: inherit;
  transition: all 0.2s ease;
  background: #f6f8fa;
}

.doc-nav-card:hover {
  border-color: #0969da;
  box-shadow: 0 4px 12px rgba(9, 105, 218, 0.1);
  transform: translateY(-2px);
}

.doc-nav-card h3 {
  margin: 0 0 10px 0;
  color: #24292f;
  font-size: 18px;
}

.doc-nav-card p {
  margin: 0;
  color: #656d76;
  font-size: 14px;
  line-height: 1.5;
}

.doc-nav-card:hover h3 {
  color: #0969da;
}
</style>
