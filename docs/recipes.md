---
layout: page
title: Recipes
permalink: /recipes/
---

# Recipes

Curated patterns and implementation guides for solving common SQLiteNow scenarios.

## Explore the guides

### Serialization Strategies
Encode and decode lists, timestamps, and enums between Kotlin types and SQLite.

[Serialization →]({{ site.baseurl }}/recipes/serialization/)

### Reactive Flows
Keep your UI synchronized with database changes using the generated flow APIs.

[Reactive Flows →]({{ site.baseurl }}/recipes/reactive-flows/)

### Inspection
Inspect generated schemas and iterate on SQL faster with persisted inspection databases.

[Inspection →]({{ site.baseurl }}/recipes/inspection/)

### Named Parameters
Ensure generated parameter types are inferred correctly for complex SQL expressions.

[Named Parameters →]({{ site.baseurl }}/recipes/named-parameters/)

### Complex Example
Walk through a multi-table view with nested results and dynamic field mapping best practices.

[Complex Example →]({{ site.baseurl }}/recipes/complex-example/)
## Quick Navigation

<div class="doc-nav-grid">
  <a href="{{ site.baseurl }}/recipes/serialization/" class="doc-nav-card">
    <h3>🔐 Serialization</h3>
    <p>Serialize lists, timestamps, and enums with type-safe adapters</p>
  </a>
  
  <a href="{{ site.baseurl }}/recipes/reactive-flows/" class="doc-nav-card">
    <h3>🔄 Reactive Flows</h3>
    <p>Drive UI updates from database changes using Flow-based APIs</p>
  </a>

  <a href="{{ site.baseurl }}/recipes/inspection/" class="doc-nav-card">
    <h3>🛠️ Inspection</h3>
    <p>Persist schema databases for inspection and SQL tooling</p>
  </a>

  <a href="{{ site.baseurl }}/recipes/named-parameters/" class="doc-nav-card">
    <h3>🏷️ Named Parameters</h3>
    <p>Guide SQLiteNow to infer precise types for complex parameter usage</p>
  </a>

  <a href="{{ site.baseurl }}/recipes/complex-example/" class="doc-nav-card">
    <h3>🧭 Complex Example</h3>
    <p>Build a view with entity, per-row, and collection mappings using clear aliases</p>
  </a>
</div>

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
