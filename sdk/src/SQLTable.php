<?php

namespace Drewlabs\ETL;

class SQLTable implements Table
{
    private $factory;
    private $name;
    private $columns;

    /**
     * Class constructor
     * 
     * @param ConnectionFactory $factory
     * @param string $name 
     * @param string|array $columns 
     */
    public function __construct(
        ConnectionFactory $factory,
        string $name,
        $columns = '*'
    ) {
        $this->factory = $factory;
        $this->name = $name;
        $this->columns = $columns;
    }

    public function add_many(array $values)
    {
        return db_insert_many($this->factory->createConnection(), $this->name, $values);
    }

    public function add(array $value)
    {
        return db_insert($this->factory->createConnection(), $this->name, $value);
    }

    public function all(string $query = null)
    {
        $columns = is_array($this->columns) ? implode(", ", $this->columns) : $this->columns;
        $sql = "
            SELECT $columns
            FROM $this->name
        ";

        if ($query) {
            $sql .= " WHERE $query";
        }
        return db_select($this->factory->createConnection(), $sql, [], true, \PDO::FETCH_ASSOC);
    }

    function exists(...$query): bool
    {
        $sql = "SELECT COUNT(*)
                FROM $this->name";
        $params = [];
        if (!empty($query)) {
            $where = [];
            foreach ($query as $q) {
                $column = trim(str_before(" ", trim($q)));
                $least = str_after(" ", $q);
                $operator = trim(str_before(" ", trim($least)));
                $value = trim(str_after(" ", $least));
                $where[] = "$column " . "$operator" . " :$column";
                $params[] = [":$column", $value, \PDO::PARAM_STR];
            }
            $sql .= " WHERE " . join(" AND", $where);
        }
        // Create and prepare PDO statement
        $stmt = $this->factory->createConnection()->prepare($sql);
        foreach ($params as $param) {
            $stmt->bindParam(...$param);
        }
        // Execute the PDO statement
        $stmt->execute();

        // Return the result of the query
        return $stmt->fetchColumn(0) !== 0;
    }
}
