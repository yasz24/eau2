# Part 1: sorer

The code for sorer consists of the internal columnar data structure, the sor format parser, and a
main file that reads command line options, runs the parser, and prints the result for the requested
query.

## Columnar data structures

The primary data structure is the BaseColumn type and its subclasses, which implement a pair of
resizable arraylists. The first is a bool array storing whether the entry at each given index is
present or missing. This is handled in BaseColumn, and it has common methods for checking if a given
entry is present and appending an item representing a missing entry to the column. Additionally,
BaseColumn has a member ColumnType, which specifies which type of column this is. This allows users
of the BaseColumn type to use dynamic_cast to a specific subtype of column in order to be able to
use the column-type-specific methods. Each subclass of BaseColumn implements a second arraylist of
the specific type of column it implements,
with as much code as possible being common to the BaseColumn parent class. The subclasses implement
public methods to append and retrieve entries of their specific column type to the column.
Additionally, the subclasses implement a pure virtual method printEntry, which prints the contents
of a given entry in the column to standard out. This allows virtual dispatch to be used by main.cpp
to print out specific entries when requested.
Additionally, a function makeColumnFromType is provided that constructs a column of the right
subclass given a ColumnType argument. The ColumnSet type stores a fixed-size collection of
BaseColumns, which may be of different types.

## Parser design

The parser is designed around two main abstractions. The StrSlice type represents a "slice" of a
larger C-style string, holding a non-owning pointer to the string as well as start and end indices.
StrSlice has methods to get individual chars, convert to a full null-terminated C-string (performing
allocation), and parse the slice contents as integer or float.
Next, the LineReader type implements an abstraction for reading files line by line, as well as the
functionality to constrain reading to given start and end indices. LineReader also handles skipping
the first and last lines automatically when start and end do not correspond to the entire file.
LineReader performs allocations for each line read, to ensure that each line has all of its contents
fully consecutive in memory, which allows StrSlice to be used during parsing on the lines. Finally,
the SorParser type contains the functionality to use LineReader to read lines in a file and
construct a ColumnSet of columns containing the parsed data. SorParser also contains a
method guessSchema which uses the first 500 lines to guess the schema of the SoR file.

## The main.cpp file

Finally, main.cpp contains the code to parse command line arguments, create an instance of
SorParser, and use it to parse the given file and print the results of the given query to standard
out.

## Tests
Some basic internal tests on columns and StrSlice are located in test.cpp. Additionally, full
integration tests on the sorer binary are run from the Makefile, and these verify sorer is able to
function on the included test sor data correctly.
