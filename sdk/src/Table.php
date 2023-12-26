<?php

namespace Drewlabs\ETL;

interface Table extends ReadOnlyTable
{
    /**
     * Add a row to the table with the provided column values
     * 
     * **Note** $value should be a dictionnary/key-value pair data structure
     * consisting of table columns as key.
     * 
     * @param array $value 
     * @return mixed 
     */
    public function add(array $value);

    /**
     * Insert a list of row into the table
     * 
     * **Note** $values should be list of dictionnary/key-value pair data structure
     * consisting of table columns as key.
     * 
     * @param array $values 
     * @return int 
     */
    public function add_many(array $values);
}
