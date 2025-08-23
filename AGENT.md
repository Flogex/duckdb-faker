## High-level Project Description
This is a DuckDB extension which can be used to generate fake data of different types.
For this, it adds new table functions to DuckDB.
These table functions take different parameters to specialize the data that is generated.
For example, for strings, minimum and maximum lengths can be specified.
As another example, the range of generated integers can be specified.
The functions return tuples with a single `value` column.
They are called like this:

```sql
SELECT value FROM random_int(min=5, max=100) LIMIT 100;
```

Note that we have to pass a LIMIT to not produce thousands of rows.


## Architecture
The extension is implemented in C++.
It uses the faker-cxx library to generate the fake data.
Catch2 is used for testing.
These libraries, together with DuckDB, are located in the submodules directory.
Code in the submodules directory should not be modified.


## Coding Style
It is important to follow the style of the existing code.
I prefer small classes in separate files.
There should be tests for all possible parameters of the table functions and they should also test the edge cases.


## Running the Executable and Tests
To build the executable, run `make release` from the root directory.
To run the tests, run `make test` from the root directory.