<?php

namespace Drewlabs\ETL\IO;

class ReadWriter
{
    /**
     * @var int|resource
     */
    private $descriptor;

    /**
     * Creates a read writer instance.
     *
     * @param mixed $descriptor
     *
     * @return void
     */
    private function __construct($descriptor)
    {
        $this->descriptor = $descriptor;
    }

    public function __destruct()
    {
        $this->close();
    }

    public static function open(string $path, $mode = 'r', $include_path = false, $context = null)
    {
        $fd = @fopen($path, $mode, $include_path, $context);
        clearstatcache(true, $path);
        if (false === $fd) {
            throw new \RuntimeException(sprintf('Error opening stream at path: %s. %s', $path, error_get_last()['message'] ?? ''));
        }

        return new static($fd);
    }

    /**
     * Read data from the open file resource.
     * 
     * **Note** Method returns false if was unable to read from
     * file resource because the resource was close or a read error
     * occurs
     * 
     * @param int|null $length
     * @param int|null $offset
     *
     * @return string|false
     */
    public function read(?int $length = null, int $offset = 0, int $operation = \LOCK_EX | \LOCK_NB)
    {
        // Case the read writer is not a resource, we simply return false
        if (!is_resource($this->descriptor)) {
            return false;
        }

        if (null === $length) {
            $length = \is_array($stats = @fstat($this->descriptor)) ? $stats['size'] : 0;
        }

        return 0 === $length ? '' : $this->readBytes($length, $operation, $offset ?? 0);
    }

    /**
     * Returns an iterable instance generated from lines in the file
     * 
     * @return \Traversable
     */
    public function getIterator()
    {
        while (false !== ($line = fgets($this->descriptor))) {
            yield $line;
        }
    }

    /**
     * Write a total bytes length to the opened file resource.
     *
     * **Note** Method returns false if was unable to write to
     * file resource because the resource was close or a write error
     * occurs
     * 
     * @param string $data 
     * @param null|int $length 
     * @param int $operation 
     * @return int|false 
     */
    public function write(string $data, ?int $length = null, $operation = \LOCK_EX | \LOCK_NB)
    {
        // Case the read writer is not a resource, we simply return false
        if (!is_resource($this->descriptor)) {
            return false;
        }
        $bytes = false;
        if ($this->descriptor && @flock($this->descriptor, $operation)) {
            $bytes = @fwrite($this->descriptor, $data, $length);
            @flock($this->descriptor, \LOCK_UN);
        }
        return $bytes;
    }

    /**
     * Closes the readable resource.
     *
     * @return void
     */
    public function close()
    {
        if (null !== $this->descriptor && is_resource($this->descriptor)) {
            fclose($this->descriptor);
            $this->descriptor = null;
        }
    }


    /**
     * Return reference to the internal stream object
     * 
     * @return int|resource 
     */
    public function getStream()
    {
        return $this->descriptor;
    }

    /**
     * Read a total of bytes from file descriptor.
     * 
     * @param int $length 
     * @param int $operation 
     * @param int|null $offset 
     * @return string|false 
     */
    private function readBytes(int $length, int $operation, int $offset = null)
    {
        $contents = false;
        if ($this->descriptor && @flock($this->descriptor, $operation)) {
            if ($offset) {
                fseek($this->descriptor, $offset);
            }
            $contents = @fread($this->descriptor, $length);
            @flock($this->descriptor, \LOCK_UN);
        }
        return $contents;
    }
}
