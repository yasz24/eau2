// Lang::CwC
#pragma once

#include <assert.h>
#include <stdio.h>
#include <string.h>

#include "../object.h"
//adapted from euhlmann
//https://github.ccs.neu.edu/euhlmann/CS4500-A1-part1

/**
 * Enum for the different types of SoR columns this code supports.
 */
enum class ColumnType { STRING, INTEGER, FLOAT, BOOL, UNKNOWN };

/**
 * Converts the given ColumnType to a string.
 * @param type the column type
 * @return A string representing this column type. Do not free or modify this string.
 */
const char* columnTypeToString(ColumnType type) {
    switch (type) {
        case ColumnType::STRING:
            return "STRING";
        case ColumnType::INTEGER:
            return "INTEGER";
        case ColumnType::FLOAT:
            return "FLOAT";
        case ColumnType::BOOL:
            return "BOOL";
        default:
            return "UNKNOWN";
    }
}

const char* columnTypeToChar(ColumnType type) {
    switch (type) {
        case ColumnType::STRING:
            return "S";
        case ColumnType::INTEGER:
            return "I";
        case ColumnType::FLOAT:
            return "F";
        case ColumnType::BOOL:
            return "B";
        default:
            return "U";
    }
}

/**
 * Base class containing common code for columns of all types
 */
class BaseColumn : public Object {
   public:
    /** Initial arraylist capacity */
    static const size_t DEFAULT_CAPACITY = 16;
    /** What kind of column this is */
    ColumnType _type;
    /** Array of booleans indicating whether entry is present or missing */
    bool* _entry_present;
    /** Length of entries in this column */
    size_t _length;
    /** Capacity of arrays before reallocation is necessary */
    size_t _capacity;

    /**
     * Constructs a BaseColumn type.
     * @param type The type of column
     * @param initial_capacity The initial capacity of the arraylists
     */
    BaseColumn(ColumnType type, size_t initial_capacity) : Object() {
        _type = type;
        _capacity = initial_capacity;
        _length = 0;
        _entry_present = new bool[_capacity];
    }

    /** Frees this BaseColumn */
    virtual ~BaseColumn() { delete[] _entry_present; }

    /**
     * Resizes the internal arrays if length has reached capacity.
     */
    virtual void _resize_if_necessary() {
        if (_length == _capacity) {
            size_t new_cap = _capacity * 2;
            _resize_entries(new_cap);
            _capacity = new_cap;
        }
    }

    /**
     * Resizes the entry present/missing bool array to a new size.
     * @param new_cap the requested size
     */
    virtual void _resize_entry_present(size_t new_cap) {
        bool* new_entry_present = new bool[new_cap];
        memcpy(new_entry_present, _entry_present, _capacity * sizeof(bool));
        delete[] _entry_present;
        _entry_present = new_entry_present;
    }

    /**
     * Marks a given entry as present or not present
     * @param which The entry index
     * @param present Whether this entry is present (true) or missing (false)
     */
    virtual void _set_entry_present(size_t which, bool present) { _entry_present[which] = present; }

    /**
     * Method to be implemented by subclasses that resizes their array of the actual entries.
     * @param new_cap The requested size. Will always be greater than the current capacity.
     */
    virtual void _resize_entries(size_t new_cap) = 0;
    /**
     * Appends a missing entry.
     */
    virtual void appendMissing() = 0;
    /**
     * Prints the entry with the given index to stdout. Internal unchecked method for subclasses to
     * implement.
     * @param which The entry index, guaranteed to represent a valid non-missing entry
     */
    virtual void _printEntry(size_t which) = 0;

    /**
     * Prints the given entry to stdout, or <MISSING> if it's missing.
     * Terminates if which >= getLength()
     * @param which The entry to print
     */
    virtual void printEntry(size_t which) {
        assert(which < _length);
        if (!isEntryPresent(which)) {
            printf("<MISSING>\n");
        } else {
            _printEntry(which);
        }
    }

    /**
     * Gets the type of this column.
     * @return The type
     */
    virtual ColumnType getType() { return _type; }

    /**
     * Returns the number of entries the column currently has.
     * @return The number of entries
     */
    virtual size_t getLength() { return _length; }

    /**
     * Checks if a given entry is present. Terminates if which >= getLength().
     * @param which The entry index
     */
    virtual bool isEntryPresent(size_t which) {
        assert(which < _length);
        return _entry_present[which];
    }
};

/**
 * Subclass of BaseColumn holding strings.
 * Expects ownership of each appended string to be transferred to the SorerStringColumn.
 * Frees contained strings in the destructor.
 */
class SorerStringColumn : public BaseColumn {
   public:
    const char** _entries;
    SorerStringColumn() : BaseColumn(ColumnType::STRING, DEFAULT_CAPACITY) {
        _entries = new const char*[_capacity];
    }

    virtual ~SorerStringColumn() {
        for (size_t i = 0; i < _length; i++) {
            if (_entries[i] != nullptr) {
                delete[] _entries[i];
            }
        }
        delete[] _entries;
    }

    virtual void _resize_entries(size_t new_cap) {
        _resize_entry_present(new_cap);
        const char** new_entries = new const char*[new_cap];
        memcpy(new_entries, _entries, _capacity * sizeof(char*));
        delete[] _entries;
        _entries = new_entries;
    }

    /**
     * Appends a given string to the column.
     * Takes ownership of entry, which means
     * references to entry after calling this method may be invalid.
     * @param entry The string. Must not be null
     */
    virtual void append(const char* entry) {
        _resize_if_necessary();
        _set_entry_present(_length, true);

        _entries[_length] = entry;

        _length += 1;
    }

    virtual void appendMissing() {
        _resize_if_necessary();
        _set_entry_present(_length, false);

        _entries[_length] = nullptr;
        _length += 1;
    }

    virtual const char* getEntry(size_t which) {
        assert(which < _length);
        if(isEntryPresent(which)) {
            return _entries[which];
        } else {
            return "";
        }
    }

    virtual void _printEntry(size_t which) { printf("\"%s\"\n", _entries[which]); }
};

/**
 * Subclass of BaseColumn holding integers.
 */
class SorerIntegerColumn : public BaseColumn {
   public:
    int* _entries;
    SorerIntegerColumn() : BaseColumn(ColumnType::INTEGER, DEFAULT_CAPACITY) {
        _entries = new int[_capacity];
    }
    virtual ~SorerIntegerColumn() { delete[] _entries; }

    virtual void _resize_entries(size_t new_cap) {
        _resize_entry_present(new_cap);
        int* new_entries = new int[new_cap];
        memcpy(new_entries, _entries, _capacity * sizeof(int));
        delete[] _entries;
        _entries = new_entries;
    }

    virtual void append(int entry) {
        _resize_if_necessary();
        _set_entry_present(_length, true);

        _entries[_length] = entry;
        _length += 1;
    }

    virtual void appendMissing() {
        _resize_if_necessary();
        _set_entry_present(_length, false);

        _entries[_length] = 0;
        _length += 1;
    }

    virtual int getEntry(size_t which) {
        assert(which < _length);
        assert(isEntryPresent(which));
        return _entries[which];
    }

    virtual void _printEntry(size_t which) { printf("%d\n", _entries[which]); }
};

/**
 * Subclass of BaseColumn holding floats.
 */
class SorerFloatColumn : public BaseColumn {
   public:
    float* _entries;
    SorerFloatColumn() : BaseColumn(ColumnType::FLOAT, DEFAULT_CAPACITY) {
        _entries = new float[_capacity];
    }
    virtual ~SorerFloatColumn() { delete[] _entries; }

    virtual void _resize_entries(size_t new_cap) {
        _resize_entry_present(new_cap);
        float* new_entries = new float[new_cap];
        memcpy(new_entries, _entries, _capacity * sizeof(float));
        delete[] _entries;
        _entries = new_entries;
    }

    virtual void append(float entry) {
        _resize_if_necessary();
        _set_entry_present(_length, true);

        _entries[_length] = entry;
        _length += 1;
    }

    virtual void appendMissing() {
        _resize_if_necessary();
        _set_entry_present(_length, false);

        _entries[_length] = 0;
        _length += 1;
    }

    virtual float getEntry(size_t which) {
        assert(which < _length);
        assert(isEntryPresent(which));
        return _entries[which];
    }

    virtual void _printEntry(size_t which) { printf("%e\n", _entries[which]); }
};

/**
 * Subclass of BaseColumn holding booleans.
 */
class SorerBoolColumn : public BaseColumn {
   public:
    bool* _entries;
    SorerBoolColumn() : BaseColumn(ColumnType::BOOL, DEFAULT_CAPACITY) {
        _entries = new bool[_capacity];
    }
    virtual ~SorerBoolColumn() { delete[] _entries; }

    virtual void _resize_entries(size_t new_cap) {
        _resize_entry_present(new_cap);
        bool* new_entries = new bool[new_cap];
        memcpy(new_entries, _entries, _capacity * sizeof(bool));
        delete[] _entries;
        _entries = new_entries;
    }

    virtual void append(bool entry) {
        _resize_if_necessary();
        _set_entry_present(_length, true);

        _entries[_length] = entry;
        _length += 1;
    }

    virtual void appendMissing() {
        _resize_if_necessary();
        _set_entry_present(_length, false);

        _entries[_length] = 0;
        _length += 1;
    }

    virtual bool getEntry(size_t which) {
        assert(which < _length);
        assert(isEntryPresent(which));
        return _entries[which];
    }

    virtual void _printEntry(size_t which) { printf("%d\n", _entries[which]); }
};

/**
 * Creates the right subclass of BaseColumn based on the given type.
 * @param type The type of column to create
 * @return The newly created column. Caller must free.
 */
BaseColumn* makeColumnFromType(ColumnType type) {
    switch (type) {
        case ColumnType::STRING:
            return new SorerStringColumn();
        case ColumnType::INTEGER:
            return new SorerIntegerColumn();
        case ColumnType::FLOAT:
            return new SorerFloatColumn();
        case ColumnType::BOOL:
            return new SorerBoolColumn();
        default:
            assert(false);
    }
}

/**
 * Represents a fixed-size set of columns of potentially different types.
 */
class ColumnSet : public Object {
   public:
    /** The array of columns */
    BaseColumn** _columns;
    /** The number of columns we have */
    size_t _length;
    /**
     * Creates a new ColumnSet that can hold the given number of columns.
     * Caller must also call initializeColumn for each column to fully initialize this class.
     * @param num_columns The max number of columns that can be held
     */
    ColumnSet(size_t num_columns) : Object() {
        _columns = new BaseColumn*[num_columns];
        _length = num_columns;
        for (size_t i = 0; i < num_columns; i++) {
            _columns[i] = nullptr;
        }
    }
    /**
     * Destructor for ColumnSet
     */
    virtual ~ColumnSet() {
        for (size_t i = 0; i < _length; i++) {
            if (_columns[i] != nullptr) {
                delete _columns[i];
            }
        }
        delete[] _columns;
    }

    /**
     * Gets the number of columns that can be held in this ColumnSet.
     * @return The number of columns
     */
    virtual size_t getLength() { return _length; }

    /**
     * Initializes the given column to the given type. Can only be called exactly once per index.
     * @param which The index for the column to initialize
     * @param type The type of column to create
     */
    virtual void initializeColumn(size_t which, ColumnType type) {
        assert(which < _length);
        assert(_columns[which] == nullptr);
        BaseColumn* col = makeColumnFromType(type);
        _columns[which] = col;
    }

    /**
     * Gets the column with the given index. initializeColumn must have been called for this index
     * first.
     * @param which The column index to retrieve
     * @return The column with the given index
     */
    virtual BaseColumn* getColumn(size_t which) {
        assert(which < _length);
        assert(_columns[which] != nullptr);
        return _columns[which];
    }
};
