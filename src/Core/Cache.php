<?php
namespace App\Core;

class Cache
{
    private static bool $useAPCu = false;
    private static string $cacheDir = '/tmp/cache/';
    
    public static function init(): void
    {
        self::$useAPCu = function_exists('apcu_enabled') && apcu_enabled();
        if (!self::$useAPCu && !is_dir(self::$cacheDir)) {
            mkdir(self::$cacheDir, 0777, true);
        }
    }
    
    public static function get(string $key)
    {
        if (self::$useAPCu) {
            $value = apcu_fetch($key, $success);
            return $success ? $value : null;
        }
        
        $file = self::$cacheDir . md5($key) . '.cache';
        if (!file_exists($file)) {
            return null;
        }
        
        $data = unserialize(file_get_contents($file));
        if ($data['expires'] < time()) {
            unlink($file);
            return null;
        }
        
        return $data['value'];
    }
    
    public static function set(string $key, $value, int $ttl = 3600): bool
    {
        if (self::$useAPCu) {
            return apcu_store($key, $value, $ttl);
        }
        
        $file = self::$cacheDir . md5($key) . '.cache';
        $data = [
            'expires' => time() + $ttl,
            'value' => $value
        ];
        
        return file_put_contents($file, serialize($data), LOCK_EX) !== false;
    }
    
    public static function delete(string $key): bool
    {
        if (self::$useAPCu) {
            return apcu_delete($key);
        }
        
        $file = self::$cacheDir . md5($key) . '.cache';
        if (file_exists($file)) {
            return unlink($file);
        }
        
        return true;
    }
}

// Инициализация кеша при загрузке
Cache::init();