<?php

namespace Drewlabs\ETL;

interface ConnectionFactory
{
    /**
     * Creates table connection instance
     * 
     * @return mixed 
     */
    public function createConnection();
}