<?php

/**
 * Get a given row in a iterable container, either an array or an iterator
 * 
 * @param array<array<string|int,mixed>>|\Iterator $list 
 * @param int $index 
 * @return mixed 
 */
function get_row($list, int $index)
{
    if (is_array($list)) {
        return $list[$index] ?? null;
    }
    foreach ($list as $key => $value) {
        if ($key === $index) {
            return $value;
        }
    }
    return null;
}

/**
 * 
 * @param mixed $frame 
 * @param int $offset 
 * @param mixed $last 
 * @return array|Generator<int, mixed, mixed, void> 
 */
function get_range($frame, $offset = 0, $last = null)
{
    if (is_array($frame)) {
        $length = (null === $last || -1 === $last) ? null : $last - $offset;
        return array_slice($frame, $offset, $length);
    }

    return get_iterator_range($frame, $offset, $last);
}

/**
 * Creates an iterable from the specified range
 * 
 * @param Traversable $iterable 
 * @param int $offset 
 * @param int $last 
 * @return Generator<int, mixed, mixed, void> 
 */
function get_iterator_range(\Traversable $iterable, $offset = 0, $last = -1)
{
    $last = $last ?? -1;
    $offset = $offset ?? 0;
    foreach ($iterable as $key => $value) {
        # code...
        if ($key >= $offset && (-1 === $last ? true : $key <= $last)) {
            yield $value;
        }
    }
}

/**
 * Generates a data frame from the column value of each row in the source data frame
 * 
 * @param array<array<string|int,mixed>>|\Iterator $list 
 * @param string|int $column 
 * @return Generator<int, mixed, mixed, void> 
 */
function get_column($list, $column)
{
    foreach ($list as $value) {
        foreach ($value as $k => $v) {
            if ($k === $column) {
                yield $v;
            }
        }
    }
}


/**
 * Creates an array from a traversable
 * 
 * @param Traversable $traversable 
 * @return array 
 */
function array_from(\Traversable $traversable)
{
    return iterator_to_array($traversable);
}
