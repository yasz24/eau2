// Lang::CwC
#pragma once

#include <stdio.h>
#include <stdlib.h>
#include "../string.h"
#include "../dataframe.h"
#include "../row.h"
#include "sorer_column.h"
//adapted from euhlmann by Chev Eldrid and Yash Shetty
//https://github.ccs.neu.edu/euhlmann/CS4500-A1-part1
/**
 * The maximum allowed length for string columns.
 */
constexpr const size_t MAX_STRING = 255;

/**
 * Represents a slice of a larger c-style string. Lightweight replacement for allocating/copying
 * regular c-style-strings.
 */
class StrSlice : public Object {
   public:
    size_t _start;
    size_t _end;
    const char* _str;
    DataFrame* df;

    /**
     * Creates a new StrSlice.
     * @param str The C-style-string to slice from. Must be valid during the lifetime of this slice
     * @param start The starting index
     * @param end The ending index
     */
    StrSlice(const char* str, size_t start, size_t end) : Object() {
        _start = start;
        _end = end;
        _str = str;
    }

    /**
     * @return The length of this slice
     */
    virtual size_t getLength() { return _end - _start; }
    /**
     * @return A non-null-terminated pointer to the chars in this slice.
     */
    virtual const char* getChars() { return &_str[_start]; }
    /**
     * Gets the char at the given index.
     * @param which The index. Must be < getLength()
     * @return The char
     */
    virtual char getChar(size_t which) {
        assert(which < _end - _start);
        return _str[_start + which];
    }

    /**
     * Updates the bounds of this slice to exclude instances of the given char at the beginning or
     * end. May shorten the length of this slice.
     * @param c The char to remove from the beginning and end
     */
    virtual void trim(char c) {
        while (_start < _end && _str[_start] == c) {
            _start++;
        }
        while (_start < _end && _str[_end - 1] == c) {
            _end--;
        }
    }

    /**
     * Allocates a new null terminated string and copies the contents of this slice to it.
     * @return The new string. Caller must free
     */
    virtual char* toCString() {
        size_t length = _end - _start;
        char* sliceCopy = new char[length + 1];
        memcpy(sliceCopy, getChars(), length);
        sliceCopy[length] = '\0';
        return sliceCopy;
    }

    /**
     * Parses the contents of this slice as an int.
     * @return An int corresponding to the digits in this slice.
     */
    virtual int toInt() {
        // Roll a custom integer parsing function to avoid having to allocate a new null-terminated
        // string for atoi and friends.
        long result = 0;
        bool is_negative = false;
        for (size_t i = _start; i < _end; i++) {
            char c = _str[i];
            if (i == _start && c == '-') {
                is_negative = true;
                continue;
            } else if (i == _start && c == '+') {
                continue;
            } else if (c >= '0' && c <= '9') {
                result = result * 10 + (c - '0');
            } else {
                break;
            }
        }
        return is_negative ? -result : result;
    }

    /**
     * Parses the contents of this slice as a float.
     * @return The float
     */
    virtual float toFloat() {
        // It's hard to roll a float parsing function by hand, so bite the bullet and allocate a
        // null-terminated string for atof.
        char* cstr = toCString();
        float result = atof(cstr);
        delete[] cstr;
        return result;
    }
};

/**
 * This class is capable of reading a given file line-by-line in a semi-efficient manner.
 * Additionally, it can be constrained to a given start and end position in the file, and will
 * discard the first and last (possibly partial) lines in this case.
 */
class LineReader : public Object {
   public:
    /** Temporary buffer for file contents */
    char _buf[4096];
    size_t _buf_length;
    /** Current position in buffer */
    size_t _pos;
    /** The file we're reading from */
    FILE* _file;
    /** Byte indices for start, end, and total file size */
    size_t _file_start;
    size_t _file_end;
    size_t _file_size;
    /** Heap-allocated current line being built by readLine() */
    char* _current_line;
    size_t _current_line_size;
    /** Total number of bytes read so far */
    size_t _read_size;

    /**
     * Constructs a new LineReader.
     * @param file The file to read from.
     * @param file_start the starting index (of bytes in the file)
     * @param file_end The ending index
     * @param file_size The total size of the file (as obtained by e.g. ftell)
     */
    LineReader(FILE* file, size_t file_start, size_t file_end, size_t file_size) : Object() {
        _file = file;
        _file_start = file_start;
        _file_end = file_end;
        _file_size = file_size;
        fseek(file, file_start, SEEK_SET);
        _current_line = nullptr;
        _current_line_size = 0;
        _pos = 0;
        _buf_length = 0;
        _read_size = 0;
    }

    /**
     * Destructor for LineReader
     */
    virtual ~LineReader() {
        if (_current_line != nullptr) {
            delete[] _current_line;
        }
    }

    /**
     * Null-terminates the current line being built and returns it, setting the _current_line
     * member to null. Ownership of the line is transferred to the caller.
     * @return A new null terminated string for the completed line
     */
    virtual char* _get_current_line() {
        if (_current_line == nullptr) {
            return nullptr;
        }

        char* line = _current_line;
        line[_current_line_size] = '\0';
        _current_line_size = 0;
        _current_line = nullptr;
        return line;
    }

    /**
     * Appends a given section of the given string to the _current_line member.
     * @param src The string to use
     * @param start The starting index in the string to copy from
     * @param end The ending index in the string to copy up to
     */
    virtual void _append_current_line(const char* src, size_t start, size_t end) {
        char* new_line;
        size_t copy_pos;
        if (_current_line == nullptr) {
            // Allocated a new heap buffer for the line
            new_line = new char[(end - start) + 1];
            _current_line_size = (end - start);
            copy_pos = 0;
        } else {
            // Expand the existing buffer
            new_line = new char[_current_line_size + (end - start) + 1];
            memcpy(new_line, _current_line, _current_line_size);
            copy_pos = _current_line_size;
            _current_line_size += (end - start);
            delete[] _current_line;
        }

        memcpy(&new_line[copy_pos], &src[start], end - start);
        _current_line = new_line;
    }

    /**
     * The main method implemented by this type. Reads the next full line out of the file. If
     * starting from a nonzero offset or ending before the end of the file, the first and last
     * lines respectively are skipped.
     * @return The next line, or nullptr if we are out of lines
     */
    virtual char* readLine() {
        bool skip_line = _read_size == 0 && _file_start != 0;

        while (true) {
            // If we need more data from the file, read it
            if (_pos >= _buf_length) {
                // If the file is over, we're done
                if (_read_size >= _file_end - _file_start || feof(_file) || ferror(_file)) {
                    char* line = _get_current_line();
                    // If a -len was provided that is less than the file size, skip the last line
                    if (_file_end != _file_size) {
                        if (line != nullptr) {
                            delete[] line;
                        }
                        return nullptr;
                    }
                    return line;
                }

                // Read up to sizeof buf data from the file
                size_t to_read = sizeof(_buf) / sizeof(char);
                // If we're up to the requested end position, only read as much as necessary
                if (_file_start + to_read > _file_end) {
                    to_read -= (_file_start + to_read) - _file_end;
                }
                _buf_length = fread(_buf, sizeof(char), to_read, _file);
                _read_size += _buf_length;

                // Start processing at the beginning of the buffer
                _pos = 0;
                if (_buf_length == 0) {
                    // If fread returned zero, go back to the start of the loop to check for
                    // feof/ferror and otherwise try reading again
                    continue;
                }
            }

            // Search for the next newline in the current buffer, and return the completed line
            // if we find one
            size_t start = _pos;
            for (size_t i = start; i < _buf_length; i++) {
                char c = _buf[i];
                if (c == '\n') {
                    _append_current_line(_buf, start, i);
                    _pos = i + 1;

                    // If we started after 0, skip the first line we read as it may be partial
                    if (skip_line) {
                        skip_line = false;
                        delete[] _get_current_line();
                        start = i + 1;
                        continue;
                    }

                    return _get_current_line();
                }
            }
            // If no newline found, loop around and read more file data
            _append_current_line(_buf, start, _buf_length);
            _pos = _buf_length;
        }
    }

    /**
     * Resets this reader. Goes back to the start position and prepares to read lines from there
     * again.
     */
    virtual void reset() {
        if (_current_line != nullptr) {
            delete[] _current_line;
        }
        _current_line = nullptr;
        _current_line_size = 0;
        fseek(_file, _file_start, SEEK_SET);
        _pos = 0;
        _buf_length = 0;
        _read_size = 0;
    }
};

/**
 * Enum representing what mode the parser is currently using for parsing.
 */
enum class ParserMode {
    /** We're trying to find the number of columns in the schema */
    DETECT_NUM_COLUMNS,
    /** We're guessing the column types */
    DETECT_SCHEMA,
    /** Parsing the whole file */
    PARSE_FILE
};

/**
 * Parses a given file into a ColumnSet with BaseColumns representing the sor data in the file.
 */
class SorParser : public Object {
   public:
    // Char constants for parsing
    static const size_t GUESS_SCHEMA_LINES = 500;
    static const char FIELD_BEGIN = '<';
    static const char FIELD_END = '>';
    static const char STRING_QUOTE = '"';
    static const char SPACE = ' ';
    static const char DOT = '.';
    static const char PLUS = '+';
    static const char MINUS = '-';

    /**
     * Checks if the given char could be part of an integer or float field.
     */
    static bool isNumeric(char c) {
        return c == MINUS || c == PLUS || c == DOT || (c >= '0' && c <= '9');
    }

    /** LineReader we're using */
    LineReader* _reader;
    /** ColumnSet for data we will ultimately parse */
    ColumnSet* _columns;
    /** Array of guesses for the types of each column in the schema */
    ColumnType* _typeGuesses;
    /** The number of columns we have detected */
    size_t _num_columns;

    /**
     * Creates a new SorParser with the given parameters.
     * @param file The file to read from.
     * @param file_start the starting index (of bytes in the file)
     * @param file_end The ending index
     * @param file_size The total size of the file (as obtained by e.g. ftell)
     */
    SorParser(FILE* file, size_t file_start, size_t file_end, size_t file_size) : Object() {
        _reader = new LineReader(file, file_start, file_end, file_size);
        _columns = nullptr;
        _typeGuesses = nullptr;
        _num_columns = 0;
    }

    /**
     * Destructor for SorParser
     */
    virtual ~SorParser() {
        delete _reader;
        if (_columns != nullptr) {
            delete _columns;
        }
        if (_typeGuesses != nullptr) {
            delete[] _typeGuesses;
        }
    }

    /**
     * Appends the next entry contained in the given StrSlice to the column at the given index,
     * using the type of the column.
     * @param slice The slice containing the data for this field
     * @param field_num The column index
     * @param columns The ColumnSet to add the data to
     */
    virtual void _appendField(StrSlice slice, size_t field_num, ColumnSet* columns) {
        slice.trim(SPACE);

        BaseColumn* column = columns->getColumn(field_num);

        if (slice.getLength() == 0) {
            column->appendMissing();
            return;
        }

        switch (column->getType()) {
            case ColumnType::STRING:
                slice.trim(STRING_QUOTE);
                assert(slice.getLength() <= MAX_STRING);
                dynamic_cast<SorerStringColumn*>(column)->append(slice.toCString());
                break;
            case ColumnType::INTEGER:
                dynamic_cast<SorerIntegerColumn*>(column)->append(slice.toInt());
                break;
            case ColumnType::FLOAT:
                dynamic_cast<SorerFloatColumn*>(column)->append(slice.toFloat());
                break;
            case ColumnType::BOOL:
                dynamic_cast<SorerBoolColumn*>(column)->append(slice.toInt() == 1);
                break;
            default:
                assert(false);
        }
    }

    /**
     * Tries to guess or update the guess for the given column index given a field contained in the
     * given StrSlice.
     * @param slice The slice to use
     * @param field_num The column index
     */
    virtual void _guessFieldType(StrSlice slice, size_t field_num) {
        slice.trim(SPACE);
        if (slice.getLength() == 0) {
            return;
        }

        // Check if the slice consists of only numeric chars
        // and specifically whether it has a . (indicating float)
        bool is_numeric = false;
        bool has_dot = false;
        for (size_t i = 0; i < slice.getLength(); i++) {
            char c = slice.getChar(i);
            if (isNumeric(c)) {
                is_numeric = true;
                if (c == DOT) {
                    has_dot = true;
                }
            }
        }
        // If the guess is already string, we can't change that because that means we have
        // seen a non-numeric entry already
        if (_typeGuesses[field_num] != ColumnType::STRING) {
            if (is_numeric && !has_dot) {
                // If it's an integer (not float), check if it's 0 or 1, which would indicate a
                // bool column
                int val = slice.toInt();
                if ((val == 0 || val == 1) && _typeGuesses[field_num] != ColumnType::INTEGER &&
                    _typeGuesses[field_num] != ColumnType::FLOAT) {
                    // Only keep the bool column guess if we haven't already guessed integer or
                    // float (because that means we have seen non-bool values)
                    _typeGuesses[field_num] = ColumnType::BOOL;
                } else if (_typeGuesses[field_num] != ColumnType::FLOAT) {
                    // Use integer guess only if we didn't already guess float (which could not be
                    // parsed as integer)
                    _typeGuesses[field_num] = ColumnType::INTEGER;
                }
            } else if (is_numeric && has_dot) {
                // If there's a dot, this must be a float column
                _typeGuesses[field_num] = ColumnType::FLOAT;
            } else {
                // If there are non-numeric chars then this must be a string column
                _typeGuesses[field_num] = ColumnType::STRING;
            }
        }
    }

    /**
     * Finds and iterates over the deliminated fields in the given line string according to the
     * given parsing mode.
     * @param line The line to scan/parse
     * @param mode The mode to use
     * @param columns The data representation to update
     */
    virtual size_t _scanLine(const char* line, ParserMode mode, ColumnSet* columns) {
        size_t num_fields = 0;
        size_t this_field_start = 0;
        bool in_field = false;
        bool in_string = false;

        // Iterate over the line, create slices for each detected field, and call either
        // _guessFieldType for ParserMode::DETECT_SCHEMA or _appendField for ParserMode::PARSE_FILE
        // for ParserMode::DETECT_NUM_COLUMNS we simply return the number of fields we saw
        for (size_t i = 0; i < strlen(line); i++) {
            char c = line[i];
            if (!in_field) {
                if (c == FIELD_BEGIN) {
                    in_field = true;
                    this_field_start = i;
                }
            } else {
                if (c == STRING_QUOTE) {
                    // Allow > inside quoted strings
                    in_string = !in_string;
                } else if (c == FIELD_END && !in_string) {
                    if (mode == ParserMode::DETECT_SCHEMA) {
                        _guessFieldType(StrSlice(line, this_field_start + 1, i), num_fields);
                    } else if (mode == ParserMode::PARSE_FILE) {
                        _appendField(StrSlice(line, this_field_start + 1, i), num_fields, columns);
                    }
                    in_field = false;
                    num_fields++;
                }
            }
        }

        return num_fields;
    }

    /**
     * Attempts to guess the schema based on the first 500 lines in the file.
     * Must be called first, before parseFile or getColumnSet. Can only be called once.
     */

    virtual void guessSchema() {
        assert(_columns == nullptr);
        assert(_typeGuesses == nullptr);
        // Detect the row with the most fields in the first 500 lines
        size_t max_columns = 0;
        for (size_t i = 0; i < GUESS_SCHEMA_LINES; i++) {
            char* next_line = _reader->readLine();
            if (next_line == nullptr) {
                break;
            }
            size_t num_columns = _scanLine(next_line, ParserMode::DETECT_NUM_COLUMNS, nullptr);
            if (num_columns > max_columns) {
                max_columns = num_columns;
            }
            delete[] next_line;
        }
        assert(max_columns != 0);

        // Guess the type for each column
        _reader->reset();
        _columns = new ColumnSet(max_columns);
        _typeGuesses = new ColumnType[max_columns];
        _num_columns = max_columns;
        for (size_t i = 0; i < _num_columns; i++) {
            _typeGuesses[i] = ColumnType::UNKNOWN;
        }

        for (size_t i = 0; i < GUESS_SCHEMA_LINES; i++) {
            char* next_line = _reader->readLine();
            if (next_line == nullptr) {
                break;
            }
            _scanLine(next_line, ParserMode::DETECT_SCHEMA, nullptr);
            delete[] next_line;
        }
        for (size_t i = 0; i < _num_columns; i++) {
            if (_typeGuesses[i] == ColumnType::UNKNOWN) {
                // Assume bool for anything we still don't have a guess for as per spec
                _typeGuesses[i] = ColumnType::BOOL;
            }
            _columns->initializeColumn(i, _typeGuesses[i]);
        }
    }

    /**
     * Parses all the data in the file (between the start index and length).
     * guessSchema() must be called before this functions. Can only be called once.
     */
    virtual void parseFile() {
        assert(_columns != nullptr);

        _reader->reset();

        char* line;
        while (true) {
            line = _reader->readLine();
            if (line == nullptr) {
                break;
            }
            size_t scanned_fields = _scanLine(line, ParserMode::PARSE_FILE, _columns);
            for (size_t i = scanned_fields; i < _num_columns; i++) {
                _columns->getColumn(i)->appendMissing();
            }
            delete[] line;
        }
    }

    /**
     * Gets the in-memory representation for the sor data.
     * guessSchema() and parseFile() must be called before this function.
     */
    virtual ColumnSet* getColumnSet() {
        assert(_columns != nullptr);

        return _columns;
    }

    /**
     * Takes the original columnSet data structure and converts it to the more modern
     * Dataframe
     */ 
    DataFrame* getDataFrame() {
        assert(_columns != nullptr);
        //generate schema
        StrBuff* sb = new StrBuff();
        for(int i = 0; i < _num_columns; i++) {
            sb->c(columnTypeToChar(_columns->getColumn(i)->getType()));
        }
        String* temp = sb->get();
        std::cout<<"schema string: "<<temp->c_str()<<"\n";
        Schema* s = new Schema(temp->c_str());
        //create new Dataframe
        DataFrame* df = new DataFrame(*s);
        //now to populate with values...
        Row* r = new Row(*s);
        for(int j = 0; j < _columns->getColumn(0)->getLength(); j++) {
            //do the thing
             for(int i = 0; i < _num_columns; i++) {
                BaseColumn* thisColumn = _columns->getColumn(i);
                switch (thisColumn->getType()) {
                    case ColumnType::STRING: {
                        SorerStringColumn* sc = dynamic_cast<SorerStringColumn*>(thisColumn);
                        String* svalue = new String(sc->getEntry(j));
                        r->set(i, svalue);
                        break;
                    }
                    case ColumnType::INTEGER: {
                        SorerIntegerColumn* ic = dynamic_cast<SorerIntegerColumn*>(thisColumn);
                        int ivalue = ic->getEntry(j);
                        r->set(i, ivalue);
                        break;
                    }
                    case ColumnType::FLOAT: {
                        SorerFloatColumn* fc = dynamic_cast<SorerFloatColumn*>(thisColumn);
                        float fvalue = fc->getEntry(j);
                        r->set(i, fvalue);
                        break;
                    }
                    case ColumnType::BOOL: {
                        SorerBoolColumn* bc = dynamic_cast<SorerBoolColumn*>(thisColumn);
                        bool bvalue = bc->getEntry(j);
                        r->set(i, bvalue);
                        break;
                    }
                    default:
                        assert(false);
                }
            }
            df->add_row(*r);
            //add row to df
        }
        return df;
    }
};
