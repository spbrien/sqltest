# SQLTest

## Installation

```bash
# Install Dependencies
brew install homebrew/versions/freetds091
pip install git+git://github.com/spbrien/sqltest.git#egg=sqltest

# Import to your project
from sqltest import SQLT
s = SQLT('mssql+pymssql://connection/string')
```

Take a look through the source code for usage details
