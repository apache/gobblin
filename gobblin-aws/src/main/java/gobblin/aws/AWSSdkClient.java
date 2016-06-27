/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */
package gobblin.aws;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.autoscaling.AmazonAutoScaling;
import com.amazonaws.services.autoscaling.AmazonAutoScalingClient;
import com.amazonaws.services.autoscaling.model.BlockDeviceMapping;
import com.amazonaws.services.autoscaling.model.CreateAutoScalingGroupRequest;
import com.amazonaws.services.autoscaling.model.CreateLaunchConfigurationRequest;
import com.amazonaws.services.autoscaling.model.InstanceMonitoring;
import com.amazonaws.services.autoscaling.model.Tag;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.AuthorizeSecurityGroupIngressRequest;
import com.amazonaws.services.ec2.model.CreateKeyPairRequest;
import com.amazonaws.services.ec2.model.CreateKeyPairResult;
import com.amazonaws.services.ec2.model.CreateSecurityGroupRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Filter;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.IpPermission;
import com.amazonaws.services.ec2.model.KeyPair;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.base.Splitter;


/**
 * This class is responsible for all AWS API calls
 *
 * @author Abhishek Tiwari
 */
public class AWSSdkClient {
  private static final Logger LOGGER = LoggerFactory.getLogger(GobblinAWSClusterLauncher.class);

  private static final Splitter SPLITTER = Splitter.on(',').omitEmptyStrings().trimResults();

  private static String AWS_ASG_SERVICE = "autoscaling";
  private static String AWS_EC2_SERVICE = "ec2";
  private static String AWS_S3_SERVICE = "s3";

  public static void createSecurityGroup(AWSClusterSecurityManager awsClusterSecurityManager,
      Regions region,
      String groupName,
      String description) {

    AmazonEC2 amazonEC2 = getEc2Client(awsClusterSecurityManager, region);
    try {
      CreateSecurityGroupRequest securityGroupRequest = new CreateSecurityGroupRequest()
          .withGroupName(groupName)
          .withDescription(description);
      amazonEC2.createSecurityGroup(securityGroupRequest);

      LOGGER.info("Created Security Group: " + groupName);
    } catch (AmazonServiceException ase) {
      // This might mean that security group is already created, hence ignore
      LOGGER.warn("Issue in creating security group", ase);
    }
  }

  public static void addPermissionsToSecurityGroup(AWSClusterSecurityManager awsClusterSecurityManager,
      Regions region,
      String groupName,
      String ipRanges,
      String ipProtocol,
      Integer fromPort,
      Integer toPort) {

    AmazonEC2 amazonEC2 = getEc2Client(awsClusterSecurityManager, region);

    IpPermission ipPermission = new IpPermission()
        .withIpRanges(ipRanges)
        .withIpProtocol(ipProtocol)
        .withFromPort(fromPort)
        .withToPort(toPort);
    AuthorizeSecurityGroupIngressRequest authorizeSecurityGroupIngressRequest =
        new AuthorizeSecurityGroupIngressRequest()
            .withGroupName(groupName)
            .withIpPermissions(ipPermission);
    amazonEC2.authorizeSecurityGroupIngress(authorizeSecurityGroupIngressRequest);

    LOGGER.info("Added permissions: " + ipPermission + " to security group: " + groupName);
  }

  public static String createKeyValuePair(AWSClusterSecurityManager awsClusterSecurityManager,
      Regions region,
      String keyName) {

    AmazonEC2 amazonEC2 = getEc2Client(awsClusterSecurityManager, region);

    CreateKeyPairRequest createKeyPairRequest = new CreateKeyPairRequest().withKeyName(keyName);
    CreateKeyPairResult createKeyPairResult = amazonEC2.createKeyPair(createKeyPairRequest);
    KeyPair keyPair = createKeyPairResult.getKeyPair();
    String material = keyPair.getKeyMaterial();
    LOGGER.info("Created key: " + keyName);
    LOGGER.info("Created material: " + material);

    return material;
  }

  public static void createLaunchConfig(AWSClusterSecurityManager awsClusterSecurityManager,
      Regions region,
      String launchConfigName,
      String imageId,
      String instanceType,
      String keyName,
      String securityGroups,
      String kernelId,
      String ramdiskId,
      BlockDeviceMapping blockDeviceMapping,
      String iamInstanceProfile,
      InstanceMonitoring instanceMonitoring,
      String userData) {

    AmazonAutoScaling autoScaling = getAmazonAutoScalingClient(awsClusterSecurityManager, region);

    CreateLaunchConfigurationRequest createLaunchConfigurationRequest = new CreateLaunchConfigurationRequest()
        .withLaunchConfigurationName(launchConfigName)
        .withImageId(imageId)
        .withInstanceType(instanceType)
        .withSecurityGroups(SPLITTER.splitToList(securityGroups))
        .withKeyName(keyName)
        .withKernelId(kernelId)
        .withRamdiskId(ramdiskId)
        .withBlockDeviceMappings(blockDeviceMapping)
        .withIamInstanceProfile(iamInstanceProfile)
        .withInstanceMonitoring(instanceMonitoring)
        .withUserData(userData);
    autoScaling.createLaunchConfiguration(createLaunchConfigurationRequest);

    LOGGER.info("Created Launch Configuration: " + launchConfigName);
  }

  public static void createAutoScalingGroup(AWSClusterSecurityManager awsClusterSecurityManager,
      Regions region,
      String groupName,
      String launchConfig,
      Integer minSize,
      Integer maxSize,
      Integer desiredCapacity,
      String availabilityZones,
      Integer cooldown,
      Integer healthCheckGracePeriod,
      String healthCheckType,
      String loadBalancer,
      Tag tag,
      String terminationPolicy) {

    AmazonAutoScaling autoScaling = getAmazonAutoScalingClient(awsClusterSecurityManager, region);

    CreateAutoScalingGroupRequest createAutoScalingGroupRequest = new CreateAutoScalingGroupRequest()
        .withAutoScalingGroupName(groupName)
        .withLaunchConfigurationName(launchConfig)
        .withMinSize(minSize)
        .withMaxSize(maxSize)
        .withDesiredCapacity(desiredCapacity)
        .withAvailabilityZones(SPLITTER.splitToList(availabilityZones))
        .withDefaultCooldown(cooldown)
        .withHealthCheckGracePeriod(healthCheckGracePeriod)
        .withHealthCheckType(healthCheckType)
        .withLoadBalancerNames(SPLITTER.splitToList(loadBalancer))
        .withTags(tag)
        .withTerminationPolicies(SPLITTER.splitToList(terminationPolicy));
    autoScaling.createAutoScalingGroup(createAutoScalingGroupRequest);

    LOGGER.info("Created AutoScalingGroup: " + groupName);
  }

  public static List<Instance> getInstancesForGroup(AWSClusterSecurityManager awsClusterSecurityManager,
      Regions region,
      String groupName,
      String status) {

    AmazonEC2 amazonEC2 = getEc2Client(awsClusterSecurityManager, region);

    final DescribeInstancesResult instancesResult = amazonEC2
        .describeInstances(new DescribeInstancesRequest()
            .withFilters(new Filter().withName("tag:aws:autoscaling:groupName").withValues(groupName)));

    List<Instance> instances = new ArrayList<>();
    for (Reservation reservation : instancesResult.getReservations()) {
      for (Instance instance : reservation.getInstances()) {
        if (null == status|| null == instance.getState()
            || status.equals(instance.getState().getName())) {
          instances.add(instance);
          LOGGER.info("Found instance: " + instance + " which qualified filter: " + status);
        } else {
          LOGGER.info("Found instance: " + instance + " but did not qualify for filter: " + status);
        }
      }
    }

    return instances;
  }

  public static List<S3ObjectSummary> listS3Bucket(AWSClusterSecurityManager awsClusterSecurityManager,
      Regions region,
      String bucketName,
      String prefix) {

    final AmazonS3 amazonS3 = getS3Client(awsClusterSecurityManager, region);

    final ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
        .withBucketName(bucketName)
        .withPrefix(prefix);

    final ObjectListing objectListing = amazonS3.listObjects(listObjectsRequest);
    LOGGER.info("S3 bucket listing for bucket: " + bucketName + " with prefix: " + prefix + " is: " + objectListing);

    return objectListing.getObjectSummaries();
  }


  public static AmazonEC2 getEc2Client(AWSClusterSecurityManager awsClusterSecurityManager,
      Regions region) {

    // TODO: Add client caching
    String ec2Endpoint = String.format("%s.%s.amazonaws.com", AWS_EC2_SERVICE, region);
    final AmazonEC2 ec2;
    if (awsClusterSecurityManager.isAssumeRoleEnabled()) {
      ec2 = new AmazonEC2Client(awsClusterSecurityManager.getBasicSessionCredentials());
    } else {
      ec2 = new AmazonEC2Client(awsClusterSecurityManager.getBasicAWSCredentials());
    }
    ec2.setEndpoint(ec2Endpoint);

    return ec2;
  }

  public static AmazonAutoScaling getAmazonAutoScalingClient(AWSClusterSecurityManager awsClusterSecurityManager,
      Regions region) {

    // TODO: Add client caching
    String autoscalingEndpoint = String.format("%s.%s.amazonaws.com", AWS_ASG_SERVICE, region);
    final AmazonAutoScaling autoScaling;
    if (awsClusterSecurityManager.isAssumeRoleEnabled()) {
      autoScaling = new AmazonAutoScalingClient(awsClusterSecurityManager.getBasicSessionCredentials());
    } else {
      autoScaling = new AmazonAutoScalingClient(awsClusterSecurityManager.getBasicAWSCredentials());
    }
    autoScaling.setEndpoint(autoscalingEndpoint);

    return autoScaling;
  }

  public static AmazonS3 getS3Client(AWSClusterSecurityManager awsClusterSecurityManager,
      Regions region) {

    // TODO: Add client caching
    String s3Endpoint;
    if (Regions.US_EAST_1.equals(region)) {
      s3Endpoint = String.format("%s.amazonaws.com", AWS_S3_SERVICE);
    } else {
      s3Endpoint = String.format("%s-%s.amazonaws.com", AWS_S3_SERVICE, region);
    }

    final AmazonS3 s3;
    if (awsClusterSecurityManager.isAssumeRoleEnabled()) {
      s3 = new AmazonS3Client(awsClusterSecurityManager.getBasicSessionCredentials());
    } else {
      s3 = new AmazonS3Client(awsClusterSecurityManager.getBasicAWSCredentials());
    }
    s3.setEndpoint(s3Endpoint);

    return s3;
  }
}
