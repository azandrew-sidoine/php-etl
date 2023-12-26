<?php

function resolve_parent_directory_path(string $path, string $base = __DIR__)
{
    $substr = substr($path, 0, 3);
    // Handle relative path
    if (('../' === $substr) || ('..\\' === $substr)) {
        $directory = $base;
        $_substr = substr($path, 0, 3);
        while (('../' === $_substr) || ('..\\' === $_substr)) {
            $directory = dirname($directory);
            $path = substr($path, 3);
            $_substr = substr($path, 0, 3);
        }
        $path = $directory . DIRECTORY_SEPARATOR . $path;
    }

    return $path;
}

function resolve_relative_path($path, string $base = __DIR__)
{
    $substr = substr($path, 0, 2);
    // Handle relative path
    if (('./' === $substr) || ('.\\' === $substr)) {
        $path = $base . DIRECTORY_SEPARATOR . substr($path, 2);
    }
    return $path;
}

function resolve_path(string $path, string $base = __DIR__)
{
    $substr = substr($path, 0, 3);
    $path = ('../' === $substr) || ('..\\' === $substr) ? resolve_parent_directory_path($path, $base) : (($subsustr = substr($substr, 0, 2)) && (('./' === $subsustr) || ('.\\' === $subsustr)) ? resolve_relative_path($path, $base) : $path);
    // If the path does not starts with '/' we append the current 
    if ('/' !== substr($path, 0, 1)) {
        $path = $base . DIRECTORY_SEPARATOR . $path;
    }
    return $path;
}