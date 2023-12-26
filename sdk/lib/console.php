<?php

if (!defined('CONSOLE_MARX_ARGS')) {
    define('CONSOLE_MARX_ARGS', 1000);
}

/**
 * Resolve command options from command arguments
 * 
 * @param array $args 
 * @return array 
 */
function console_get_options(array $args)
{
    $index = 0;
    $configs = array();
    while ($index < CONSOLE_MARX_ARGS && isset($args[$index])) {
        if (preg_match('/^([^-\=]+.*)$/', $args[$index], $matches) === 1) {
            // not have ant -= prefix
            $configs[$matches[1]] = true;
        } else if (preg_match('/^-+(.+)$/', $args[$index], $matches) === 1) {
            // match prefix - with next parameter
            if (preg_match('/^-+(.+)\=(.+)$/', $args[$index], $subMatches) === 1) {
                if (array_key_exists($subMatches[1], $configs)) {
                    $configs[$subMatches[1]] = array_merge(is_array($configs[$subMatches[1]]) ? $configs[$subMatches[1]] : [$configs[$subMatches[1]]], [$subMatches[2]]);
                } else {
                    $configs[$subMatches[1]] = $subMatches[2];
                }
            } else if (isset($args[$index + 1]) && preg_match('/^[^-\=]+$/', $args[$index + 1]) === 1) {
                // have sub parameter
                if (array_key_exists($matches[1], $configs)) {
                    $configs[$matches[1]] = array_merge(is_array($configs[$matches[1]]) ? $configs[$matches[1]] : [$configs[$matches[1]]], [$args[$index + 1]]);
                } else {
                    $configs[$matches[1]] = $args[$index + 1];
                }
                $index++;
            } elseif (strpos($matches[0], '--') === false) {
                for ($j = 0; $j < strlen($matches[1]); $j += 1) {
                    $configs[$matches[1][$j]] = true;
                }
            } else if (isset($args[$index + 1]) && preg_match('/^[^-].+$/', $args[$index + 1]) === 1) {
                if (array_key_exists($matches[1], $configs)) {
                    $configs[$matches[1]] = array_merge(is_array($configs[$matches[1]]) ? $configs[$matches[1]] : [$configs[$matches[1]]], [$args[$index + 1]]);
                } else {
                    $configs[$matches[1]] = $args[$index + 1];
                }
                $index++;
            } else {
                $configs[$matches[1]] = true;
            }
        }
        $index++;
    }

    return $configs;
}
