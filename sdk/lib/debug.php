<?php

/**
 * Log variable to console an stop script exection
 * 
 * @param mixed $value 
 * @return never 
 */
function dd(...$value)
{
    if (!empty($value)) {
        var_dump($value);
    }
    die();
}
