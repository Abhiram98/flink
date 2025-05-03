package org.apache.flink.api.common;

import com.esotericsoftware.kryo.Serializer;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;

public class SerializerConfig implements Serializable {
    LinkedHashMap<Class<?>, ExecutionConfig.SerializableSerializer<?>> registeredTypesWithKryoSerializers =
            new LinkedHashMap<Class<?>, ExecutionConfig.SerializableSerializer<?>>();
    LinkedHashMap<Class<?>, Class<? extends Serializer<?>>>
            registeredTypesWithKryoSerializerClasses = new LinkedHashMap<Class<?>, Class<? extends Serializer<?>>>();
    LinkedHashMap<Class<?>, ExecutionConfig.SerializableSerializer<?>> defaultKryoSerializers =
            new LinkedHashMap<Class<?>, ExecutionConfig.SerializableSerializer<?>>();
    LinkedHashMap<Class<?>, Class<? extends Serializer<?>>> defaultKryoSerializerClasses =
            new LinkedHashMap<Class<?>, Class<? extends Serializer<?>>>();
    LinkedHashSet<Class<?>> registeredKryoTypes = new LinkedHashSet<Class<?>>();
    LinkedHashSet<Class<?>> registeredPojoTypes = new LinkedHashSet<Class<?>>();

    public SerializerConfig() {
    }
}
