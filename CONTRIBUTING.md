# Get Started!

Here's how to set up `lomnia-ingester` for local development.
Please note this documentation assumes you already have `uv` and `Git` installed and ready to go.

1. Fork the `lomnia-ingester` repo on GitHub.

2. Clone your fork locally:

```bash
cd <directory_in_which_repo_should_be_created>
git clone git@github.com:YOUR_NAME/lomnia-ingester.git
```

3. Now we need to install the environment. Navigate into the directory

```bash
cd lomnia-ingester
```

Then, install and activate the environment with:

```bash
uv sync
```

4. Install pre-commit to run linters/formatters at commit time:

```bash
uv run pre-commit install
```

5. When you're done making changes, check that your changes pass the formatting tests.

```bash
make check
```
