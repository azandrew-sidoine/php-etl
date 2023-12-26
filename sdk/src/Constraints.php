<?php

namespace Drewlabs\ETL;

class Constraints
{

    public function __construct(private array $unique)
    {
    }

    /**
     * Returns the list of columns that might be unique
     * 
     * @return array 
     */
    public function getUniqueConstraints()
    {
        return $this->unique;
    }
}
