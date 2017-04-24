## Facilities spider

### Getting Started

```bash
conda create --name facilities python=3.6 # Create a virtual env if you wish.
source activate facilities                # Activate it
pip install -r requirements.text          # Just requests library.
```

### Running the Spider

Edit `ids.json` to reflect the ids you wish to crawl, or populate it
programmatically.

```json
[
    "110005239241",
    "110007449369",
    "110017951643"
]
```

Run the spider to populate `output.csv`.

```bash
python facilities.py
```

Note: The CSV file is not handling for duplicates. So if you populate it,
remove entries before running again (keep the header) or remove duplicates
downstream (you can filter dupes by id, `pandas.DataFrame.drop_duplicates`
seems ideal here).

### Bug Reporting

Please report issues if you find them.
