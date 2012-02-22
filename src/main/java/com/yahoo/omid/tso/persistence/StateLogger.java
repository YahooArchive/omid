package com.yahoo.omid.tso.persistence;

interface StateLogger {
    
    /**
     * Initializes the storage subsystem to add
     * new records.
     */
    void initialize();

    
    /**
     * Add a new record.
     */
    void addRecord();

}
