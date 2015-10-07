/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.compaction.verify;

import java.util.List;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import gobblin.compaction.Dataset;
import gobblin.compaction.verify.DataCompletenessVerifier.Results;
import gobblin.compaction.verify.DataCompletenessVerifier.Results.Result;
import gobblin.configuration.State;


/**
 * A class that simulates data completeness verification for Gobblin compaction. It randomly decides
 * whether the verification of a {@link Dataset} passes or fails.
 */
public class SimulatedDataCompletenessVerifier extends DataCompletenessVerifier.AbstractRunner {

  private static final Logger LOG = LoggerFactory.getLogger(SimulatedDataCompletenessVerifier.class);

  private static final String SIMULATED_VERIFIER_MIN_VERIFICATION_TIME_SEC =
      "simulated.verifier.min.verification.time.sec";
  private static final int DEFAULT_SIMULATED_VERIFIER_MIN_VERIFICATION_TIME_SEC = 10;
  private static final String SIMULATED_VERIFIER_MAX_VERIFICATION_TIME_SEC =
      "simulated.verifier.max.verification.time.sec";
  private static final int DEFAULT_SIMULATED_VERIFIER_MAX_VERIFICATION_TIME_SEC = 240;
  private static final String SIMULATED_VERIFIER_PASS_PROBABILITY = "simulated.verifier.pass.probability";
  private static final double DEFAULT_SIMULATED_VERIFIER_PASS_PROBABILITY = 2.0 / 3.0;
  private static final Random RANDOM = new Random();

  public SimulatedDataCompletenessVerifier(Iterable<Dataset> datasets, State props) {
    super(datasets, props);
  }

  @Override
  public Results call() throws Exception {
    int minTime = getMinVerificationTime();
    int maxTime = getMaxVerificationTime();
    Preconditions.checkState(minTime >= 0, SIMULATED_VERIFIER_MIN_VERIFICATION_TIME_SEC + " should be nonnegative.");
    Preconditions.checkState(minTime <= maxTime, SIMULATED_VERIFIER_MIN_VERIFICATION_TIME_SEC
        + " should be no more than " + SIMULATED_VERIFIER_MAX_VERIFICATION_TIME_SEC);
    long verifTime = RANDOM.nextInt(maxTime - minTime) + minTime;
    LOG.info("Verifying data completeness for " + this.datasets + ", will take " + verifTime + " sec");
    try {
      Thread.sleep(verifTime * 1000);
    } catch (InterruptedException e) {
      LOG.error("Interrupted", e);
    }

    List<Result> results = Lists.newArrayList();
    for (Dataset dataset : this.datasets) {
      boolean passed = verificationPassed();
      if (passed) {
        results.add(new Result(dataset, Result.Status.PASSED));
      } else {
        results.add(new Result(dataset, Result.Status.FAILED));
      }
    }
    return new Results(results);
  }

  private int getMinVerificationTime() {
    return this.props.getPropAsInt(SIMULATED_VERIFIER_MIN_VERIFICATION_TIME_SEC,
        DEFAULT_SIMULATED_VERIFIER_MIN_VERIFICATION_TIME_SEC);
  }

  private int getMaxVerificationTime() {
    return this.props.getPropAsInt(SIMULATED_VERIFIER_MAX_VERIFICATION_TIME_SEC,
        DEFAULT_SIMULATED_VERIFIER_MAX_VERIFICATION_TIME_SEC);
  }

  private boolean verificationPassed() {
    return RANDOM.nextDouble() < getPassProbability() ? true : false;
  }

  private double getPassProbability() {
    return this.props.getPropAsDouble(SIMULATED_VERIFIER_PASS_PROBABILITY, DEFAULT_SIMULATED_VERIFIER_PASS_PROBABILITY);
  }
}
