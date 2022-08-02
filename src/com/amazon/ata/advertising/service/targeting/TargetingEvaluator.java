package com.amazon.ata.advertising.service.targeting;

import com.amazon.ata.advertising.service.model.RequestContext;
import com.amazon.ata.advertising.service.targeting.predicate.TargetingPredicate;
import com.amazon.ata.advertising.service.targeting.predicate.TargetingPredicateResult;
import com.amazon.ata.advertising.service.util.Futures;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Evaluates TargetingPredicates for a given RequestContext.
 */
public class TargetingEvaluator {
    public static final boolean IMPLEMENTED_STREAMS = true;
    public static final boolean IMPLEMENTED_CONCURRENCY = true;
    private final RequestContext requestContext;

    /**
     * Creates an evaluator for targeting predicates.
     * @param requestContext Context that can be used to evaluate the predicates.
     */
    public TargetingEvaluator(RequestContext requestContext) {
        this.requestContext = requestContext;
    }

    /**
     * Evaluate a TargetingGroup to determine if all of its TargetingPredicates are TRUE or not for the given
     * RequestContext.
     * @param targetingGroup Targeting group for an advertisement, including TargetingPredicates.
     * @return TRUE if all of the TargetingPredicates evaluate to TRUE against the RequestContext, FALSE otherwise.
     */
    public TargetingPredicateResult evaluate(TargetingGroup targetingGroup) {
        ExecutorService executor = Executors.newCachedThreadPool();

        List<TargetingPredicateResult> results = new ArrayList<>();

        targetingGroup.getTargetingPredicates()
                .forEach(targetingPredicate -> executor.submit(() -> results.add(targetingPredicate.evaluate(requestContext))));

        executor.shutdown();
        try {
            executor.awaitTermination(11000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            System.out.println("Thread was interrupted: " + this);
        }

        return results.stream()
                .anyMatch(targetingPredicateResult -> !targetingPredicateResult.isTrue()) ? TargetingPredicateResult.FALSE : TargetingPredicateResult.TRUE;
    }
}
