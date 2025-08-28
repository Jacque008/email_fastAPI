# Refactored OOP wrapper

This package makes your workflow truly object-oriented by **wrapping the input
emails DataFrame into an `EmailDataset` object** and the **errands into an
`ErrandDataset` object**, and by exposing domain actions as methods on those
objects (e.g., `clean()`, `detect_sender()`, `extract_numbers()`, `connect()`).
It reuses your existing functional modules in `oop/` under the hood.

## Install / Use

Your project layout might look like:

```
your_project/
  oop/                      # your existing modules (Processor, Detector, Connector, ...)
  refactored_oop/
    email_workflow/
      __init__.py
      services.py
      email_dataset.py
      errand_dataset.py
      errand_connector.py
      pipeline.py
```

### Quickstart

```python
import pandas as pd
from email_workflow import EmailDataset, ErrandDataset, EmailProcessingPipeline

# 1) Wrap incoming emails
emails = EmailDataset(df)

# 2) Option A: Inline, fluent chaining
emails.clean() \      .detect_sender() \      .detect_receiver() \      .handle_vendor_specials() \      .extract_numbers() \      .extract_attachments() \      .categorize()

# 3) Prepare errands (preloaded from DB or passed in externally)
errands = ErrandDataset.recent_default()   # or ErrandDataset.from_db("er.\"createdAt\" >= NOW() - INTERVAL '45 day'")

# 4) Connect
emails.connect(errands=errands)

# 5) Use the resulting DataFrame
out_df = emails.to_frame()

# Or do everything in one shot:
pipe = EmailProcessingPipeline()
out_df = pipe.run(df, errands=errands, do_categorize=True, do_enrich_staff_animal=False)
```

### Why this is *truly* OOP

- **Data as objects**: `EmailDataset` and `ErrandDataset` hold *state* (their `DataFrame`) and
  *behavior* (methods that operate on that state).
- **Fluent API**: Every method returns `self`, enabling readable pipelines.
- **Dependency injection**: `DefaultServices` lazily instantiates the heavy modules so you can
  unit-test by swapping in stubs/mocks if needed.
- **Composable connection**: You can pass an `ErrandDataset` to `EmailDataset.connect()` to avoid
  DB roundtrips and keep matching logic deterministic and testable.

### Notes

- The adapters reuse your original `Connector`'s matching functions; no business rules were changed.
- `DefaultServices` will initialize `oop` modules on first use; make sure your env vars / secrets are set.
- You can extend `EmailDataset` with additional domain methods without touching the underlying services.
