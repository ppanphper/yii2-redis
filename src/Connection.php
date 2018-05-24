<?php
/**
 * @link http://www.heyanlong.com/
 * @copyright Copyright (c) 2015 heyanlong.com
 * @license http://www.heyanlong.com/license/
 */

namespace ppanphper\redis;

use Yii;
use yii\base\Component;
use yii\base\UnknownMethodException;
use \Exception;
use \Redis;
use \RedisException;
use \RedisCluster;

class Connection extends Component
{

    const EVENT_AFTER_OPEN = 'afterOpen';

    // phpRedis客户端
    const CLIENT_TYPE_PHP_REDIS = 1;
    // phpRedis cluster 客户端
    const CLIENT_TYPE_PHP_REDIS_CLUSTER = 2;

    /**
     * 获取Redis客户端对象方法映射
     * @var array
     */
    protected $_clientTypeObjectMap = [
        self::CLIENT_TYPE_PHP_REDIS         => 'getRedisObject',
        self::CLIENT_TYPE_PHP_REDIS_CLUSTER => 'getRedisClusterObject',
    ];

    /**
     * @var string the hostname or ip address to use for connecting to the redis server. Defaults to 'localhost'.
     * If [[unixSocket]] is specified, hostname and [[port]] will be ignored.
     */
    public $hostname = 'localhost';
    /**
     * @var integer the port to use for connecting to the redis server. Default port is 6379.
     * If [[unixSocket]] is specified, [[hostname]] and port will be ignored.
     */
    public $port = 6379;

    /**
     * @var string the password for establishing DB connection. Defaults to null meaning no AUTH command is sent.
     * See http://redis.io/commands/auth
     */
    public $password;
    /**
     * @var integer the redis database to use. This is an integer value starting from 0. Defaults to 0.
     * Since version 2.0.6 you can disable the SELECT command sent after connection by setting this property to `null`.
     */
    public $database = 0;
    /**
     * @var float timeout to use for connection to redis. If not set the timeout set in php.ini will be used: `ini_get("default_socket_timeout")`.
     */
    public $connectionTimeout = null;
    /**
     * @var float timeout to use for redis socket when reading and writing data. If not set the php default value will be used.
     */
    public $dataTimeout = null;

    /**
     * @var bool
     */
    public $persistent = false;

    /**
     * cluster nodes
     *
     * @var array
     *
     * [
     *  'hostname:port',
     *  'hostname:port',
     *  ...
     * ]
     */
    public $servers = [];

    /**
     * client type
     *
     * @var int
     */
    public $clientType = self::CLIENT_TYPE_PHP_REDIS_CLUSTER;

    /**
     * 是否使用igbinary扩展序列化/反序列化
     *
     * @var bool
     */
    public $useIGBinary = false;

    /**
     * Redis instance
     *
     * @var
     */
    private $_redis;

    /**
     * Key prefix
     *
     * @var string
     */
    public $prefix = '';

    /**
     * 脚本列表
     * @var array
     */
    protected $_funcTable = [
        // 增量计数器，第一次增量计数的时候，给key加上过期时间，解决并发问题 eg: evalScript('incr', 'key', expireTime)
        'incr'       => [
            'sha1'   => '727c0136efce8e1e7b34a5d1a29c87b77a9348ff',
            'script' => "local count = redis.call('incr',KEYS[1]); if tonumber(count) == 1 then redis.call('expire',KEYS[1],ARGV[1]); end; return count;"
        ],
        // 增量计数器，并在增量值超过最大值时，重置为0 eg: evalScript('incr_reset', 'key', [maxCounter，expireTime])
        'incr_reset' => [
            'sha1'   => '064e70749675e1c315270a18e5c38ae3f314498a',
            'script' => "local count = redis.call('incr',KEYS[1]); if tonumber(count) == 1 then redis.call('expire',KEYS[1],ARGV[2]); end; if tonumber(count) > tonumber(ARGV[1]) then redis.call('set', KEYS[1], 0); return 0; end; return count;",
        ],
        // 增量计数器，如果当前值没有大于限定值，才可以加一并返回[1, 累加后的值]，否则返回[0, 当前值] eg: evalScript('incr_max', 'key', [maxCounter, expireTime])
        'incr_max'   => [
            'sha1'   => '56a52dbab84bd9b0fc0a8330caff45c31d2df9ab',
            'script' => "local count = redis.call('get',KEYS[1]); if ( count == false or tonumber(count) < tonumber(ARGV[1]) ) then count = redis.call('incr', KEYS[1]); if count == 1 then redis.call('expire',KEYS[1],ARGV[2]); end; return {1, count}; else return {0, count}; end;",
        ],
        // 存在才将 key 中储存的数字值减一 eg: evalScript('decr_exist', 'key')
        'decr_exist' => [
            'sha1'   => 'b8fdb9f741719829325bcc7253b93eed7b526ccb',
            'script' => "local count = redis.call('exists',KEYS[1]); if tonumber(count) == 1 then count = redis.call('decr',KEYS[1]); end; return count;"
        ],
    ];

    public function init()
    {
        // 是否支持Redis
        if (!$this->is_supported()) {
            throw new Exception("Redis class is not exists, Please make sure is installed!");
        }
    }

    public function getHandle()
    {
        if ($this->_redis == null) {
            $this->_redis = $this->{$this->_clientTypeObjectMap[$this->clientType]}();
            $this->initConnection();
        }
        return $this->_redis;
    }

    /**
     * 获取已经与Redis建立连接的对象
     * @return Redis
     *
     * @throws Exception
     * @throws RedisException
     */
    private function getRedisObject()
    {
        $redis = new Redis();
        $connectMethod = $this->persistent ? 'pconnect' : 'connect';

        $success = $redis->{$connectMethod}($this->hostname, $this->port, $this->connectionTimeout);
        // 是否连接成功
        if ($success) {
            // 连接成功但校验密码失败
            if (!empty($this->password) && !$redis->auth($this->password)) {
                // 密码验证不通过，那么就不再重连尝试了
                throw new RedisException('Redis authentication failed.');
            }
            if ($this->database !== null && !$redis->select($this->database)) {
                // 密码验证不通过，那么就不再重连尝试了
                throw new RedisException('Redis select database failed.');
            }
        } else {
            throw new RedisException('Redis connection failed. Check your configuration.');
        }
        $redis->setOption(Redis::OPT_PREFIX, $this->prefix);
        $redis->setOption(Redis::OPT_SERIALIZER, $this->getSerializerType());
        return $redis;
    }

    /**
     * 获取PHP Redis Cluster
     * @return RedisCluster
     */
    private function getRedisClusterObject()
    {
        $redisCluster = new RedisCluster(null, $this->servers, $this->connectionTimeout, $this->dataTimeout, $this->persistent);
        // In the event we can't reach a master, and it has slaves, failover for read commands
        $redisCluster->setOption(RedisCluster::OPT_SLAVE_FAILOVER, RedisCluster::FAILOVER_DISTRIBUTE_SLAVES);
        $redisCluster->setOption(RedisCluster::OPT_PREFIX, $this->prefix);
        $redisCluster->setOption(RedisCluster::OPT_SERIALIZER, $this->getSerializerType());
        return $redisCluster;
    }

    /**
     * 获取序列化类型
     * @return int
     */
    private function getSerializerType()
    {
        $redisOptSerializer = Redis::SERIALIZER_PHP;
        // 如果安装了快速序列化扩展，就使用扩展
        if ($this->is_supported_igbinary() && $this->useIGBinary === true) {
            $redisOptSerializer = Redis::SERIALIZER_IGBINARY;
        }
        return $redisOptSerializer;
    }

    /**
     * @param $scriptKey
     * @param array|string $keys 键名
     * @param array|string $args 参数
     *
     * @return bool|mixed
     * @throws Exception
     *
     * @example
     * client->evalScript('incr', 'test', 100); // incr, key, 过期时间
     * 增量计数器，超过最大值就重置为0
     * client->evalScript('incr_reset', 'test', [maxCounter, 100]); // incr_reset, key, [最大值, 过期时间]
     * 增量计数器，如果当前值没有大于限定值，才可以加一并返回[1, 累加后的值]，否则返回[0, 当前值]
     * client->evalScript('incr_max', 'test', [maxCounter, 100]); // incr_max, key, [最大值, 过期时间]
     *
     */
    public function evalScript($scriptKey, $keys, $args=[]) {
        if(!isset($this->_funcTable[$scriptKey]) || empty($this->_funcTable[$scriptKey]['script'])) {
            throw new Exception(__METHOD__ . ' please configure '.$scriptKey.' script');
        }
        if(empty($this->_funcTable[$scriptKey]['sha1'])) {
            $this->_funcTable[$scriptKey]['sha1'] = sha1($this->_funcTable[$scriptKey]['script']);
        }
        $sha1 = $this->_funcTable[$scriptKey]['sha1'];
        $result = false;
        try {
            if ($this->getHandle() === null) throw new Exception('Redis connection failed. Check your configuration.');

            if(!is_array($keys)) {
                $keys = [$keys];
            }
            if(!is_array($args)) {
                $args = [$args];
            }
            $keyCount = count($keys);
            // 不需要键名索引，用数字重新建立索引
            $args = array_values(array_merge($keys, $args));
            for($i =0; $i < 2; $i++) {
                $result = $this->_redis->evalSha($sha1, $args, $keyCount);
                if($result === false && $i === 0) {
                    $errorMsg = $this->_redis->getLastError();
                    $this->_redis->clearLastError();
                    // 该脚本不存在该节点上，需要执行load
                    if(stripos($errorMsg, 'NOSCRIPT') !== false) {
                        // 单机
                        if($this->clientType == self::CLIENT_TYPE_PHP_REDIS) {
                            $loadParams = [
                                'load',
                                $this->_funcTable[$scriptKey]['script']
                            ];
                        }
                        // 集群
                        else {
                            // 取Key用来定位节点
                            $key = $keys;
                            if(is_array($keys)) {
                                $key = $keys[0];
                            }
                            $loadParams = [
                                $key,
                                'load',
                                $this->_funcTable[$scriptKey]['script']
                            ];
                        }
                        // load脚本
                        $serverSha1 = $this->_redis->script(...$loadParams);
                        // 在开发阶段解决这个错误
                        if($serverSha1 !== $sha1) {
                            throw new Exception($scriptKey.' The SHA1 of the script is inconsistent with the SHA1 returned by the server. '.$sha1.'=='.$serverSha1, 999);
                        }
                        continue;
                    }
                }
                break;
            }
        } catch (Exception $e) {
            // 如果是开发阶段能解决的错误，就抛出去
            if($e->getCode() === 999) {
                throw new Exception($e->getMessage(), $e->getCode());
            }
            $result = false;
            Yii::warning(__METHOD__.' = '. $e->getMessage());
        }
        return $result;
    }

    /**
     * 检测是否支持igbinary扩展, 用于数据序列化
     * @return bool
     */
    public function is_supported_igbinary()
    {
        return extension_loaded('igbinary');
    }

    /**
     * 设置Key前缀
     *
     * @param $prefix
     */
    public function setPrefix($prefix)
    {
        $this->getHandle()->setOption(Redis::OPT_PREFIX, $prefix);
    }

    /**
     * __call magic method
     *
     * Handles access to the parent driver library's methods
     *
     * @access    public
     *
     * @param    string
     * @param    array
     *
     * @return    mixed
     */
    public function __call($method, $args)
    {
        try {
            $handle = $this->getHandle();
            if ($handle === null) throw new Exception('Redis connection failed. Check your configuration.');
            if (method_exists($handle, $method))
                return $handle->{$method}(...$args);
            throw new UnknownMethodException('Calling unknown method: ' . get_class($handle) . "::$method()");
        } catch (UnknownMethodException $e) {
            throw new UnknownMethodException($e->getMessage());
        } catch (Exception $e) {
            Yii::warning('Redis::' . $method . ' = ' . $e->getMessage());
            return false;
        }
    }

    /**
     * 关闭连接
     */
    public function close()
    {
        if ($this->_redis) {
            try {
                switch ($this->clientType) {
                    case self::CLIENT_TYPE_PHP_REDIS:
                    case self::CLIENT_TYPE_PHP_REDIS_CLUSTER:
                        $this->_redis->close();
                        break;
                }
            } catch (Exception $e) {
                Yii::warning(__METHOD__ . ' = ' . $e->getMessage());
            }
        }
        $this->_redis = null;
    }

    /**
     * Returns the name of the DB driver for the current [[dsn]].
     * @return string name of the DB driver
     */
    public function getDriverName()
    {
        return 'redis';
    }

    /**
     * @return LuaScriptBuilder
     */
    public function getLuaScriptBuilder()
    {
        return new LuaScriptBuilder();
    }

    /**
     * Returns a value indicating whether the DB connection is established.
     * @return bool whether the DB connection is established
     */
    public function getIsActive()
    {
        return $this->_redis !== null;
    }

    public function __sleep()
    {
        $this->close();

        return array_keys(get_object_vars($this));
    }

    protected function initConnection()
    {
        $this->trigger(self::EVENT_AFTER_OPEN);
    }

    /**
     * Check if Redis driver is supported
     *
     * @return    bool
     */
    public function is_supported()
    {
        // 如果使用PRedis，可以不需要装Redis扩展
        return extension_loaded('redis');
    }

    /**
     * @param string|array|null $key 集群模式ping需要参数
     *
     * @return bool|string
     */
    public function ping($key = null)
    {
        $result = false;
        try {
            switch ($this->clientType) {
                // PHPRedis
                case self::CLIENT_TYPE_PHP_REDIS:
                    $result = $this->_redis->ping();
                    break;
                // PHPRedis Cluster
                case self::CLIENT_TYPE_PHP_REDIS_CLUSTER:
                    /**
                     * proto bool RedisCluster::ping(string key)
                     * proto bool RedisCluster::ping(array host_port)   eg: [(string)'host', (int)port]
                     */
                    $result = $this->_redis->ping($key);
                    break;
            }
        } catch (\Exception $e) {
            Yii::warning(__METHOD__ . ' = ' . $e->getMessage());
        }
        return $result;
    }

    /**
     * @return bool aways return true
     */
    public function flushDB()
    {
        Yii::warning('flushDB: never flush redis');
        return true;
    }

    /**
     * @return bool aways return true
     */
    public function flushAll()
    {
        Yii::warning('flushAll: never flush redis');
        return true;
    }

}