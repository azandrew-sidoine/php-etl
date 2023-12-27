<?php

namespace Drewlabs\ETL;

class Constraints
{

    private $unique;

    public function __construct(array $unique)
    {
        $this->unique = $unique;
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
