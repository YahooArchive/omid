package com.yahoo.omid.tso.persistence;


import com.yahoo.omid.tso.TSOState;

/**
 * This is an interface for mechanisms that implement
 * the recovery of the TSO state. 
 *
 */

interface StateBuilder {

    /**
     * This call should create a new TSOState object and populate
     * it accordingly. If there was an incarnation of TSO in the past,
     * then this call recovers the state to populate the TSOState
     * object.
     * 
     * 
     * @return a new TSOState
     */
    TSOState initialize();
    
}
