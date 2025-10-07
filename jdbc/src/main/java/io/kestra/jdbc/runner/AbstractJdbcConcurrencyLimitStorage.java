package io.kestra.jdbc.runner;

import io.kestra.core.models.flows.FlowInterface;
import io.kestra.core.runners.ConcurrencyLimit;
import io.kestra.core.runners.ExecutionRunning;
import io.kestra.jdbc.repository.AbstractJdbcRepository;
import org.apache.commons.lang3.tuple.Pair;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.impl.DSL;

import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;

public class AbstractJdbcConcurrencyLimitStorage extends AbstractJdbcRepository {
    protected io.kestra.jdbc.AbstractJdbcRepository<ConcurrencyLimit> jdbcRepository;

    public AbstractJdbcConcurrencyLimitStorage(io.kestra.jdbc.AbstractJdbcRepository<ConcurrencyLimit> jdbcRepository) {
        this.jdbcRepository = jdbcRepository;
    }

    /**
     * Fetch the concurrency limit counter then process the count using the consumer function.
     * It locked the raw and is wrapped in a transaction so the consumer should use the provided dslContext for any database access.
     * <p>
     * Note that to avoid a race when no concurrency limit counter exists, it first always try to insert a 0 counter.
     */
    public ExecutionRunning countThenProcess(FlowInterface flow, BiFunction<DSLContext, ConcurrencyLimit, Pair<ExecutionRunning, ConcurrencyLimit>> consumer) {
        return this.jdbcRepository
            .getDslContextWrapper()
            .transactionResult(configuration -> {
                var dslContext = DSL.using(configuration);

                // insert ignore to avoid a race when there is nothing in DB
                var zeroConcurrencyLimit = ConcurrencyLimit.builder()
                    .tenantId(flow.getTenantId())
                    .namespace(flow.getNamespace())
                    .flowId(flow.getId())
                    .running(0)
                    .build();
                Map<Field<Object>, Object> fields = this.jdbcRepository.persistFields(zeroConcurrencyLimit);
                dslContext.insertInto(this.jdbcRepository.getTable())
                    .set(io.kestra.jdbc.repository.AbstractJdbcRepository.field("key"), this.jdbcRepository.key(zeroConcurrencyLimit))
                    .set(fields)
                    .onDuplicateKeyIgnore()
                    .execute();

                var select = dslContext
                    .select()
                    .from(this.jdbcRepository.getTable())
                    .where(this.buildTenantCondition(flow.getTenantId()))
                    .and(field("namespace").eq(flow.getNamespace()))
                    .and(field("flow_id").eq(flow.getId()));

                var selected = this.jdbcRepository.map(select.forUpdate().fetchOne());
                var pair = consumer.apply(dslContext, selected);
                save(dslContext, pair.getRight());
                return pair.getLeft();
            });
    }

    /**
     * Decrement the concurrency limit counter.
     * Must only be called when a flow having concurrency limit ends.
     */
    public void decrement(FlowInterface flow) {
        this.jdbcRepository
            .getDslContextWrapper()
            .transaction(configuration -> {
                var dslContext = DSL.using(configuration);

                fetchOne(dslContext, flow).ifPresent(
                    concurrencyLimit -> save(dslContext, concurrencyLimit.withRunning(concurrencyLimit.getRunning() == 0 ? 0 : concurrencyLimit.getRunning() - 1))
                );
            });
    }

    /**
     * Increment the concurrency limit counter.
     * Must only be called when a queued execution is popped, other use cases must pass thought the standard process of creating an execution.
     */
    public void increment(DSLContext dslContext, FlowInterface flow) {
        fetchOne(dslContext, flow).ifPresent(
            concurrencyLimit -> save(dslContext, concurrencyLimit.withRunning(concurrencyLimit.getRunning() + 1))
        );
    }

    private Optional<ConcurrencyLimit> fetchOne(DSLContext dslContext, FlowInterface flow) {
        var select = dslContext
            .select()
            .from(this.jdbcRepository.getTable())
            .where(this.buildTenantCondition(flow.getTenantId()))
            .and(field("namespace").eq(flow.getNamespace()))
            .and(field("flow_id").eq(flow.getId()));

        return Optional.ofNullable(select.forUpdate().fetchOne())
            .map(record -> this.jdbcRepository.map(record));
    }

    private void save(DSLContext dslContext, ConcurrencyLimit concurrencyLimit) {
        Map<Field<Object>, Object> fields = this.jdbcRepository.persistFields(concurrencyLimit);
        this.jdbcRepository.persist(concurrencyLimit, dslContext, fields);
    }
}
