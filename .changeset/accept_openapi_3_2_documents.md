---
default: minor
---

# Accept OpenAPI 3.2 documents

Documents declaring `openapi: 3.2.*` are now accepted instead of rejected. They are read the same way as 3.1
documents, so features new in 3.2, such as `itemSchema` for sequential media types like `text/event-stream`, are not
used yet. A response described only by `itemSchema` generates as a response without a body.
