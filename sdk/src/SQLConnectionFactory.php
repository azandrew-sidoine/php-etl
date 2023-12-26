<?php

namespace Drewlabs\ETL;

class SQLConnectionFactory implements ConnectionFactory
{
    /**
     * @var \PDO
     */
    private $pdo;

    public function __construct(
        private string $user = 'docker',
        private string $password = 'docker',
        private string $driver = 'mysql',
        private string $dns_or_host = '0.0.0.0',
        private int $port = 3306,
        private ?string $dbname = null,
        private string $charset = 'UTF8'
    ) {
    }

    /**
     * Creates sql connection from dictionnary of configuration
     * 
     * @param array $array 
     * @return static 
     */
    public static function fromArray(array $array)
    {
        return new static(
            $array['user'] ?? 'docker',
            $array['password'] ?? 'password',
            $array['driver'] ?? 'mysql',
            $array['host'] ?? '0.0.0.0',
            intval($array['port'] ?? 3306),
            $array['db'] ?? null
        );
    }

    /**
     * {@inheritDoc}
     * 
     * @return \PDO
     */
    public function createConnection()
    {
        // Reuse the pdo connection if it's already created
        if (null === $this->pdo) {
            $this->pdo = create_database_connection(
                $this->user,
                $this->password,
                $this->dns_or_host,
                $this->driver,
                $this->dbname,
                $this->port,
                $this->charset
            );
        }

        // Returns the created pdo connection
        return $this->pdo;
    }
}
