---
default: minor
---

# Accept OpenAPI 3.2 documents

Documents declaring `openapi: 3.2.*` are now accepted instead of rejected. They are read the same way as 3.1
documents, plus `itemSchema` on `text/event-stream` responses; other features new in 3.2 are not used yet.
