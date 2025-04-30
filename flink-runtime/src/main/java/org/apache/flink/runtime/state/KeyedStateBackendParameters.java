package org.apache.flink.runtime.state;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;

import javax.annotation.Nonnull;
import java.util.Collection;

public class KeyedStateBackendParameters<K> {
    private final Environment env;
    private final JobID jobID;
    private final String operatorIdentifier;
    private final TypeSerializer<K> keySerializer;
    private final int numberOfKeyGroups;
    private final KeyGroupRange keyGroupRange;
    private final TaskKvStateRegistry kvStateRegistry;
    private final TtlTimeProvider ttlTimeProvider;
    private final MetricGroup metricGroup;
    @Nonnull
    private final Collection<KeyedStateHandle> stateHandles;
    private final CloseableRegistry cancelStreamRegistry;

    /**
     * @param env The environment of the task.
     * @param jobID The ID of the job that the task belongs to.
     * @param operatorIdentifier The identifier text of the operator.
     * @param keySerializer The key-serializer for the operator.
     * @param numberOfKeyGroups The number of key-groups aka max parallelism.
     * @param keyGroupRange Range of key-groups for which the to-be-created backend is responsible.
     * @param kvStateRegistry KvStateRegistry helper for this task.
     * @param ttlTimeProvider Provider for TTL logic to judge about state expiration.
     * @param metricGroup The parent metric group for all state backend metrics.
     * @param stateHandles The state handles for restore.
     * @param cancelStreamRegistry The registry to which created closeable objects will be
     *     registered during restore.
     * @param <K> The type of the keys by which the state is organized.
     *
     */
    public KeyedStateBackendParameters(Environment env, JobID jobID, String operatorIdentifier, TypeSerializer<K> keySerializer, int numberOfKeyGroups, KeyGroupRange keyGroupRange, TaskKvStateRegistry kvStateRegistry, TtlTimeProvider ttlTimeProvider, MetricGroup metricGroup, @Nonnull Collection<KeyedStateHandle> stateHandles, CloseableRegistry cancelStreamRegistry) {
        this.env = env;
        this.jobID = jobID;
        this.operatorIdentifier = operatorIdentifier;
        this.keySerializer = keySerializer;
        this.numberOfKeyGroups = numberOfKeyGroups;
        this.keyGroupRange = keyGroupRange;
        this.kvStateRegistry = kvStateRegistry;
        this.ttlTimeProvider = ttlTimeProvider;
        this.metricGroup = metricGroup;
        this.stateHandles = stateHandles;
        this.cancelStreamRegistry = cancelStreamRegistry;
    }

    public Environment getEnv() {
        return env;
    }

    public JobID getJobID() {
        return jobID;
    }

    public String getOperatorIdentifier() {
        return operatorIdentifier;
    }

    public TypeSerializer<K> getKeySerializer() {
        return keySerializer;
    }

    public int getNumberOfKeyGroups() {
        return numberOfKeyGroups;
    }

    public KeyGroupRange getKeyGroupRange() {
        return keyGroupRange;
    }

    public TaskKvStateRegistry getKvStateRegistry() {
        return kvStateRegistry;
    }

    public TtlTimeProvider getTtlTimeProvider() {
        return ttlTimeProvider;
    }

    public MetricGroup getMetricGroup() {
        return metricGroup;
    }

    public Collection<KeyedStateHandle> getStateHandles() {
        return stateHandles;
    }

    public CloseableRegistry getCancelStreamRegistry() {
        return cancelStreamRegistry;
    }
}
