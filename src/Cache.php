<?php
/**
 * @link http://www.yiiframework.com/
 * @copyright Copyright (c) 2008 Yii Software LLC
 * @license http://www.yiiframework.com/license/
 */

namespace ppanphper\redis;

use Yii;
use yii\base\Component;
use yii\caching\CacheInterface;
use yii\di\Instance;
use yii\caching\Dependency;
use yii\helpers\StringHelper;

/**
 * Redis Cache implements a cache application component based on [redis](http://redis.io/) key-value store.
 *
 * Redis Cache requires redis version 2.8.0 or higher to work properly.
 *
 * It needs to be configured with a redis [[Connection]] that is also configured as an application component.
 * By default it will use the `redis` application component.
 *
 * See [[Cache]] manual for common cache operations that redis Cache supports.
 *
 * Unlike the [[Cache]], redis Cache allows the expire parameter of [[set]], [[add]], [[mset]] and [[madd]] to
 * be a floating point number, so you may specify the time in milliseconds (e.g. 0.1 will be 100 milliseconds).
 *
 * To use redis Cache as the cache application component, configure the application as follows,
 *
 * ~~~
 * [
 *     'components' => [
 *         'cache' => [
 *             'class' => 'ppanphper\redis\Cache',
 *             'redis' => [
 *                 'hostname' => 'localhost',
 *                 'port' => 6379,
 *                 'database' => 0,
 *             ]
 *         ],
 *     ],
 * ]
 * ~~~
 *
 * Or if you have configured the redis [[Connection]] as an application component, the following is sufficient:
 *
 * ~~~
 * [
 *     'components' => [
 *         'cache' => [
 *             'class' => 'ppanphper\redis\Cache',
 *             // 'redis' => 'redis' // id of the connection application component
 *         ],
 *     ],
 * ]
 * ~~~
 *
 * @author DongYang Pan <flowphp@gmail.com>
 * @since 1.0
 */
class Cache extends \yii\caching\Cache
{
    /**
     * @var Connection|string|array the Redis [[Connection]] object or the application component ID of the Redis [[Connection]].
     * This can also be an array that is used to create a redis [[Connection]] instance in case you do not want do configure
     * redis connection as an application component.
     * After the Cache object is created, if you want to change this property, you should only assign it
     * with a Redis [[Connection]] object.
     */
    public $redis = 'redis';

    /**
     * @var string a string prefixed to every cache key so that it is unique globally in the whole cache storage.
     * It is recommended that you set a unique cache key prefix for each application if the same cache
     * storage is being used by different applications.
     *
     * To ensure interoperability, only alphanumeric characters should be used.
     */
    public $keyPrefix;

    /**
     * @var null|array|false the functions used to serialize and unserialize cached data. Defaults to null, meaning
     * using the default PHP `serialize()` and `unserialize()` functions. If you want to use some more efficient
     * serializer (e.g. [igbinary](http://pecl.php.net/package/igbinary)), you may configure this property with
     * a two-element array. The first element specifies the serialization function, and the second the deserialization
     * function. If this property is set false, data will be directly sent to and retrieved from the underlying
     * cache component without any serialization or deserialization. You should not turn off serialization if
     * you are using [[Dependency|cache dependency]], because it relies on data serialization. Also, some
     * implementations of the cache can not correctly save and retrieve data different from a string type.
     */
    public $serializer = false;

    /**
     * @var int default duration in seconds before a cache entry will expire. Default value is 0, meaning infinity.
     * This value is used by [[set()]] if the duration is not explicitly given.
     * @since 2.0.11
     */
    public $defaultDuration = 0;



    /**
     * Initializes the redis Cache component.
     * This method will initialize the [[redis]] property to make sure it refers to a valid redis connection.
     * @throws \yii\base\InvalidConfigException if [[redis]] is invalid.
     */
    public function init()
    {
        parent::init();
        $this->redis = Instance::ensure($this->redis, Connection::className());
    }

    /**
     * Checks whether a specified key exists in the cache.
     * This can be faster than getting the value from the cache if the data is big.
     * In case a cache does not support this feature natively, this method will try to simulate it
     * but has no performance improvement over getting it.
     * Note that this method does not check whether the dependency associated
     * with the cached data, if there is any, has changed. So a call to [[get]]
     * may return false while exists returns true.
     *
     * @param mixed $key a key identifying the cached value. This can be a simple string or
     * a complex data structure consisting of factors representing the key.
     *
     * @return bool true if a value exists in cache, false if the value is not in the cache or expired.
     */
    public function exists($key)
    {
        $key = $this->buildKey($key);
        return $this->redis->exists($key);
    }

    /**
     * @inheritdoc
     */
    protected function getValue($key)
    {
        return $this->redis->get($key);
    }

    /**
     * @inheritdoc
     */
    protected function getValues($keys)
    {
        $result = $this->redis->mget($keys);
        return $result;
    }

    /**
     * @inheritdoc
     */
    protected function setValue($key, $value, $expire)
    {
        if ($expire == 0) {
            return (bool) $this->redis->set($key, $value);
        } else {
            $expire = (int) ($expire * 1000);

            return (bool) $this->redis->set($key, $value, ['PX'=>$expire]);
        }
    }

    /**
     * @inheritdoc
     */
    protected function setValues($data, $expire)
    {
        $failedKeys = [];
        if ($expire == 0) {
            $this->redis->mset($data);
        } else {
            $expire = (int)($expire * 1000);
            $this->redis->multi();
            $this->redis->mset($data);
            $index = [];
            foreach ($data as $key => $value) {
                $this->redis->pexpire($key, $expire);
                $index[] = $key;
            }
            $result = $this->redis->exec();
            array_shift($result);
            foreach ($result as $i => $r) {
                if ($r != 1) {
                    $failedKeys[] = $index[$i];
                }
            }
        }

        return $failedKeys;
    }

    /**
     * @inheritdoc
     */
    protected function addValue($key, $value, $expire)
    {
        if ($expire == 0) {
            return $this->redis->set($key, $value, ['NX']);
        }
        $expire = (int)($expire * 1000);
        return $this->redis->set($key, $value, ['PX' => $expire, 'NX']);
    }

    /**
     * @inheritdoc
     */
    protected function deleteValue($key)
    {
        return (bool) $this->redis->del($key);
    }

    /**
     * Deletes all values from cache.
     * Be careful of performing this operation if the cache is shared among multiple applications.
     * @return bool whether the flush operation was successful.
     */
    public function flush()
    {
        return $this->flushValues();
    }

    /**
     * @inheritdoc
     */
    protected function flushValues()
    {
        return $this->redis->flushDB();
    }
}
