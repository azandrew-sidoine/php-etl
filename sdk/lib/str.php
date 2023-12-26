<?php

use Random\RandomException;

/**
 * 
 * @param string $character 
 * @param string $haystack 
 * @return string 
 */
function str_before(string $character, string $haystack)
{
    if ($pos = strpos($haystack, $character)) {
        return substr($haystack, 0, $pos);
    }
    return '';
}

/**
 * 
 * @param string $character 
 * @param string $haystack 
 * @return string 
 */
function str_after(string $character, string $haystack)
{
    if (!\is_bool(strpos($haystack, $character))) {
        return substr($haystack, strpos($haystack, $character) + strlen($character));
    }
    return '';
}

/**
 * Random UUID generator
 * 
 * @return string 
 * @throws RandomException 
 */
function str_uuid() {
    return strtolower(sprintf(
        '%04X%04X-%04X-%04X-%04X-%04X%04X%04X',
        random_int(0, 65535),
        random_int(0, 65535),
        random_int(0, 65535),
        random_int(16384, 20479),
        random_int(32768, 49151),
        random_int(0, 65535),
        random_int(0, 65535),
        random_int(0, 65535)
    ));
}