<?php

namespace Drewlabs\ETL;

interface ReadOnlyTable
{
    /**
     * Return the list of table rows to be processed
     * 
     * **Note** The $query parameter is provided for implementation
     * classes to filter the output data based on some criteria
     * 
     * @param string $query 
     * @return array|\Traversable 
     */
    public function all(string $query = null);
    
    /**
     * Execute a query that check if a row matching the query exists
     * 
     * **Note** List of query that are used during the search. Queries are in form of columnOperatorValue
     * As an example, (label=Hello World)
     * 
     * @param string $query 
     * 
     * @return bool
     */
    public function exists(...$query): bool;
}