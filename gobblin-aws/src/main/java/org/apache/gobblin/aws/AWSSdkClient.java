/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package gobblin.aws;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.regions.Region;
import com.amazonaws.services.autoscaling.AmazonAutoScaling;
import com.amazonaws.services.autoscaling.AmazonAutoScalingClient;
import com.amazonaws.services.autoscaling.model.AutoScalingGroup;
import com.amazonaws.services.autoscaling.model.BlockDeviceMapping;
import com.amazonaws.services.autoscaling.model.CreateAutoScalingGroupRequest;
import com.amazonaws.services.autoscaling.model.CreateLaunchConfigurationRequest;
import com.amazonaws.services.autoscaling.model.DeleteAutoScalingGroupRequest;
import com.amazonaws.services.autoscaling.model.DeleteLaunchConfigurationRequest;
import com.amazonaws.services.autoscaling.model.DescribeAutoScalingGroupsRequest;
import com.amazonaws.services.autoscaling.model.InstanceMonitoring;
import com.amazonaws.services.autoscaling.model.Tag;
import com.amazonaws.services.autoscaling.model.TagDescription;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.AuthorizeSecurityGroupIngressRequest;
import com.amazonaws.services.ec2.model.AvailabilityZone;
import com.amazonaws.services.ec2.model.CreateKeyPairRequest;
import com.amazonaws.services.ec2.model.CreateKeyPairResult;
import com.amazonaws.services.ec2.model.CreateSecurityGroupRequest;
import com.amazonaws.services.ec2.model.DescribeAvailabilityZonesResult;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Filter;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.IpPermission;
import com.amazonaws.services.ec2.model.KeyPair;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.Lists;

import gobblin.annotation.Alpha;


/**
 * Class responsible for all AWS API calls.
 *
 * <p>
 *   This class makes use of AWS SDK API and provides clients for various Amazon AWS services
 *   such as: EC2, S3, AutoScaling; as well as this class provides various helper methods to
 *   perform AWS service API calls.
 * </p>
 *
 * @author Abhishek Tiwari
 */
@Alpha
public class AWSSdkClient {
  private static final Logger LOGGER = LoggerFactory.getLogger(GobblinAWSClusterLauncher.class);

  private static final Splitter SPLITTER = Splitter.on(',').omitEmptyStrings().trimResults();

  private Supplier<AmazonEC2> amazonEC2Supplier;
  private Supplier<AmazonS3> amazonS3Supplier;
  private Supplier<AmazonAutoScaling> amazonAutoScalingSupplier;

  /***
   * Initialize the AWS SDK Client
   *
   * @param awsClusterSecurityManager The {@link AWSClusterSecurityManager} to fetch AWS credentials
   * @param region The Amazon AWS {@link Region}
   */
  public AWSSdkClient(final AWSClusterSecurityManager awsClusterSecurityManager, final Region region) {
    this.amazonEC2Supplier = Suppliers.memoize(new Supplier<AmazonEC2>() {
      @Override
      public AmazonEC2 get() {
        AmazonEC2Client amazonEC2 = new AmazonEC2Client(awsClusterSecurityManager.getCredentialsProvider());
        amazonEC2.setRegion(region);
        return amazonEC2;
      }
    });
    this.amazonS3Supplier = Suppliers.memoize(new Supplier<AmazonS3>() {
      @Override
      public AmazonS3 get() {
        AmazonS3Client amazonS3 = new AmazonS3Client(awsClusterSecurityManager.getCredentialsProvider());
        amazonS3.setRegion(region);
        return amazonS3;
      }
    });
    this.amazonAutoScalingSupplier = Suppliers.memoize(new Supplier<AmazonAutoScaling>() {
      @Override
      public AmazonAutoScaling get() {
        AmazonAutoScalingClient amazonAutoScaling =
                new AmazonAutoScalingClient(awsClusterSecurityManager.getCredentialsProvider());
        amazonAutoScaling.setRegion(region);
        return amazonAutoScaling;
      }
    });
  }

  /***
   * Create an Amazon AWS security group
   *
   * @param groupName Security group name
   * @param description Security group description
   */
  public void createSecurityGroup(String groupName,
      String description) {

    AmazonEC2 amazonEC2 = getEc2Client();
    try {
      final CreateSecurityGroupRequest securityGroupRequest = new CreateSecurityGroupRequest()
          .withGroupName(groupName)
          .withDescription(description);
      amazonEC2.createSecurityGroup(securityGroupRequest);

      LOGGER.info("Created Security Group: " + groupName);
    } catch (AmazonServiceException ase) {
      // This might mean that security group is already created, hence ignore
      LOGGER.warn("Issue in creating security group", ase);
    }
  }

  /***
   * Open firewall for a security group
   *
   * @param groupName Open firewall for this security group
   * @param ipRanges Open firewall for this IP range
   * @param ipProtocol Open firewall for this protocol type (eg. tcp, udp)
   * @param fromPort Open firewall for port range starting at this port
   * @param toPort Open firewall for port range ending at this port
   */
  public void addPermissionsToSecurityGroup(String groupName,
      String ipRanges,
      String ipProtocol,
      Integer fromPort,
      Integer toPort) {

    final AmazonEC2 amazonEC2 = getEc2Client();

    final IpPermission ipPermission = new IpPermission()
        .withIpRanges(ipRanges)
        .withIpProtocol(ipProtocol)
        .withFromPort(fromPort)
        .withToPort(toPort);
    final AuthorizeSecurityGroupIngressRequest authorizeSecurityGroupIngressRequest =
        new AuthorizeSecurityGroupIngressRequest()
            .withGroupName(groupName)
            .withIpPermissions(ipPermission);
    amazonEC2.authorizeSecurityGroupIngress(authorizeSecurityGroupIngressRequest);

    LOGGER.info("Added permissions: " + ipPermission + " to security group: " + groupName);
  }

  /***
   * Creates a 2048-bit RSA key pair with the specified name
   *
   * @param keyName Key name to use
   * @return Unencrypted PEM encoded PKCS#8 private key
   */
  public String createKeyValuePair(String keyName) {

    final AmazonEC2 amazonEC2 = getEc2Client();

    final CreateKeyPairRequest createKeyPairRequest = new CreateKeyPairRequest().withKeyName(keyName);
    final CreateKeyPairResult createKeyPairResult = amazonEC2.createKeyPair(createKeyPairRequest);
    final KeyPair keyPair = createKeyPairResult.getKeyPair();
    final String material = keyPair.getKeyMaterial();
    LOGGER.info("Created key: " + keyName);
    LOGGER.debug("Created material: " + material);

    return material;
  }

  /***
   * Create a launch configuration that can be later used to create {@link AmazonAutoScaling} groups
   *
   * @param launchConfigName Desired launch config name
   * @param imageId AMI image id to use
   * @param instanceType EC2 instance type to use
   * @param keyName Key name
   * @param securityGroups Security groups to apply
   * @param kernelId Optional kernel id
   * @param ramdiskId Optional ram disk id
   * @param blockDeviceMapping Optional EBS device mapping
   * @param iamInstanceProfile Optional IAM instance profile
   * @param instanceMonitoring Optional instance monitoring
   * @param userData User data (eg. shell script to execute at instance boot under this launch config)
   */
  public void createLaunchConfig(String launchConfigName,
      String imageId,
      String instanceType,
      String keyName,
      String securityGroups,
      Optional<String> kernelId,
      Optional<String> ramdiskId,
      Optional<BlockDeviceMapping> blockDeviceMapping,
      Optional<String> iamInstanceProfile,
      Optional<InstanceMonitoring> instanceMonitoring,
      String userData) {

    final AmazonAutoScaling autoScaling = getAmazonAutoScalingClient();

    CreateLaunchConfigurationRequest createLaunchConfigurationRequest = new CreateLaunchConfigurationRequest()
        .withLaunchConfigurationName(launchConfigName)
        .withImageId(imageId)
        .withInstanceType(instanceType)
        .withSecurityGroups(SPLITTER.splitToList(securityGroups))
        .withKeyName(keyName)
        .withUserData(userData);
    if (kernelId.isPresent()) {
      createLaunchConfigurationRequest = createLaunchConfigurationRequest
          .withKernelId(kernelId.get());
    }
    if (ramdiskId.isPresent()) {
      createLaunchConfigurationRequest = createLaunchConfigurationRequest
          .withRamdiskId(ramdiskId.get());
    }
    if (blockDeviceMapping.isPresent()) {
      createLaunchConfigurationRequest = createLaunchConfigurationRequest
          .withBlockDeviceMappings(blockDeviceMapping.get());
    }
    if (iamInstanceProfile.isPresent()) {
      createLaunchConfigurationRequest = createLaunchConfigurationRequest
          .withIamInstanceProfile(iamInstanceProfile.get());
    }
    if (instanceMonitoring.isPresent()) {
      createLaunchConfigurationRequest = createLaunchConfigurationRequest
          .withInstanceMonitoring(instanceMonitoring.get());
    }

    autoScaling.createLaunchConfiguration(createLaunchConfigurationRequest);

    LOGGER.info("Created Launch Configuration: " + launchConfigName);
  }

  /***
   * Delete a launch configuration by its name
   *
   * @param launchConfigName Name of launch config to delete
   */
  public void deleteLaunchConfiguration(String launchConfigName) {

    final AmazonAutoScaling autoScaling = getAmazonAutoScalingClient();

    final DeleteLaunchConfigurationRequest deleteLaunchConfigurationRequest = new DeleteLaunchConfigurationRequest()
        .withLaunchConfigurationName(launchConfigName);

    autoScaling.deleteLaunchConfiguration(deleteLaunchConfigurationRequest);

    LOGGER.info("Deleted Launch Configuration: " + launchConfigName);
  }

  /***
   * Create and launch an {@link AmazonAutoScaling} group
   *
   * @param groupName Auto scaling group name
   * @param launchConfig Launch configuration string
   * @param minSize Minimum number of instances to maintain in auto scaling group
   * @param maxSize Maximum number of instances to scale up-to for load
   * @param desiredCapacity Desired number of instances to maintain in auto scaling group
   * @param availabilityZones Optional availability zones to make use of
   * @param cooldown Optional cooldown period before any scaling event (default is 300 secs)
   * @param healthCheckGracePeriod Optional grace period till which no health check is performed after bootup (default is 300 secs)
   * @param healthCheckType Optional health check type (default is EC2 instance check)
   * @param loadBalancer Optional load balancer to use
   * @param terminationPolicy Optional termination policies
   * @param tags Optional tags to set on auto scaling group (they are set to propagate to EC2 instances implicitly)
   */
  public void createAutoScalingGroup(String groupName,
      String launchConfig,
      Integer minSize, Integer maxSize, Integer desiredCapacity,
      Optional<String> availabilityZones,
      Optional<Integer> cooldown,
      Optional<Integer> healthCheckGracePeriod,
      Optional<String> healthCheckType,
      Optional<String> loadBalancer,
      Optional<String> terminationPolicy,
      List<Tag> tags) {

    AmazonAutoScaling autoScaling = getAmazonAutoScalingClient();

    // Propagate ASG tags to EC2 instances launched under the ASG by default
    // (we want to ensure this, hence not configurable)
    final List<Tag> tagsWithPropagationSet = Lists.newArrayList();
    for (Tag tag : tags) {
      tagsWithPropagationSet.add(tag.withPropagateAtLaunch(true));
    }

    CreateAutoScalingGroupRequest createAutoScalingGroupRequest = new CreateAutoScalingGroupRequest()
        .withAutoScalingGroupName(groupName)
        .withLaunchConfigurationName(launchConfig)
        .withMinSize(minSize)
        .withMaxSize(maxSize)
        .withDesiredCapacity(desiredCapacity)
        .withTags(tagsWithPropagationSet);
    if (availabilityZones.isPresent()) {
      createAutoScalingGroupRequest = createAutoScalingGroupRequest
          .withAvailabilityZones(SPLITTER.splitToList(availabilityZones.get()));
    }
    if (cooldown.isPresent()) {
      createAutoScalingGroupRequest = createAutoScalingGroupRequest
          .withDefaultCooldown(cooldown.get());
    }
    if (healthCheckGracePeriod.isPresent()) {
      createAutoScalingGroupRequest = createAutoScalingGroupRequest
          .withHealthCheckGracePeriod(healthCheckGracePeriod.get());
    }
    if (healthCheckType.isPresent()) {
      createAutoScalingGroupRequest = createAutoScalingGroupRequest
          .withHealthCheckType(healthCheckType.get());
    }
    if (loadBalancer.isPresent()) {
      createAutoScalingGroupRequest = createAutoScalingGroupRequest
          .withLoadBalancerNames(SPLITTER.splitToList(loadBalancer.get()));
    }
    if (terminationPolicy.isPresent()) {
      createAutoScalingGroupRequest = createAutoScalingGroupRequest
          .withTerminationPolicies(SPLITTER.splitToList(terminationPolicy.get()));
    }

    autoScaling.createAutoScalingGroup(createAutoScalingGroupRequest);

    LOGGER.info("Created AutoScalingGroup: " + groupName);
  }

  /***
   * Delete an auto scaling group by its name
   *
   * @param autoScalingGroupName Name of auto scaling group to delete
   * @param shouldForceDelete If the AutoScalingGroup should be deleted without waiting for instances to terminate
   */
  public void deleteAutoScalingGroup(String autoScalingGroupName,
      boolean shouldForceDelete) {

    final AmazonAutoScaling autoScaling = getAmazonAutoScalingClient();

    final DeleteAutoScalingGroupRequest deleteLaunchConfigurationRequest = new DeleteAutoScalingGroupRequest()
        .withAutoScalingGroupName(autoScalingGroupName)
        .withForceDelete(shouldForceDelete);

    autoScaling.deleteAutoScalingGroup(deleteLaunchConfigurationRequest);

    LOGGER.info("Deleted AutoScalingGroup: " + autoScalingGroupName);
  }

  /***
   * Get list of {@link AutoScalingGroup}s for a given tag
   *
   * @param tag Tag to filter the auto scaling groups
   * @return List of {@link AutoScalingGroup}s qualifying the filter tag
   */
  public List<AutoScalingGroup> getAutoScalingGroupsWithTag(Tag tag) {

    final AmazonAutoScaling autoScaling = getAmazonAutoScalingClient();

    final DescribeAutoScalingGroupsRequest describeAutoScalingGroupsRequest = new DescribeAutoScalingGroupsRequest();

    final List<AutoScalingGroup> allAutoScalingGroups = autoScaling
        .describeAutoScalingGroups(describeAutoScalingGroupsRequest)
        .getAutoScalingGroups();

    final List<AutoScalingGroup> filteredAutoScalingGroups = Lists.newArrayList();
    for (AutoScalingGroup autoScalingGroup : allAutoScalingGroups) {
      for (TagDescription tagDescription : autoScalingGroup.getTags()) {
        if (tagDescription.getKey().equalsIgnoreCase(tag.getKey()) &&
            tagDescription.getValue().equalsIgnoreCase(tag.getValue())) {
          filteredAutoScalingGroups.add(autoScalingGroup);
        }
      }
    }

    return filteredAutoScalingGroups;
  }

  /***
   * Get list of EC2 {@link Instance}s for a auto scaling group
   *
   * @param groupName Auto scaling group name
   * @param status Instance status (eg. running)
   * @return List of EC2 instances found for the input auto scaling group
   */
  public List<Instance> getInstancesForGroup(String groupName,
      String status) {

    final AmazonEC2 amazonEC2 = getEc2Client();

    final DescribeInstancesResult instancesResult = amazonEC2.describeInstances(new DescribeInstancesRequest()
        .withFilters(new Filter().withName("tag:aws:autoscaling:groupName").withValues(groupName)));

    final List<Instance> instances = new ArrayList<>();
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

  /***
   * Get availability zones in an Amazon AWS region
   *
   * @return List of availability zones
   */
  public List<AvailabilityZone> getAvailabilityZones() {

    final AmazonEC2 amazonEC2 = getEc2Client();

    final DescribeAvailabilityZonesResult describeAvailabilityZonesResult = amazonEC2.describeAvailabilityZones();
    final List<AvailabilityZone> availabilityZones = describeAvailabilityZonesResult.getAvailabilityZones();
    LOGGER.info("Found: " + availabilityZones.size() + " availability zone");

    return availabilityZones;
  }

  /***
   * Download a S3 object to local directory
   *
   * @param s3ObjectSummary S3 object summary for the object to download
   * @param targetDirectory Local target directory to download the object to
   * @throws IOException If any errors were encountered in downloading the object
   */
  public void downloadS3Object(S3ObjectSummary s3ObjectSummary,
      String targetDirectory)
      throws IOException {

    final AmazonS3 amazonS3 = getS3Client();

    final GetObjectRequest getObjectRequest = new GetObjectRequest(
        s3ObjectSummary.getBucketName(),
        s3ObjectSummary.getKey());

    final S3Object s3Object = amazonS3.getObject(getObjectRequest);

    final String targetFile = StringUtils.removeEnd(targetDirectory, File.separator) + File.separator + s3Object.getKey();
    FileUtils.copyInputStreamToFile(s3Object.getObjectContent(), new File(targetFile));

    LOGGER.info("S3 object downloaded to file: " + targetFile);
  }

  /***
   * Get list of S3 objects within a S3 bucket qualified by prefix path
   *
   * @param bucketName S3 bucket name
   * @param prefix S3 prefix to object
   * @return List of {@link S3ObjectSummary} objects within the bucket qualified by prefix path
   */
  public List<S3ObjectSummary> listS3Bucket(String bucketName,
      String prefix) {

    final AmazonS3 amazonS3 = getS3Client();

    final ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
        .withBucketName(bucketName)
        .withPrefix(prefix);

    final ObjectListing objectListing = amazonS3.listObjects(listObjectsRequest);
    LOGGER.info("S3 bucket listing for bucket: " + bucketName + " with prefix: " + prefix + " is: " + objectListing);

    return objectListing.getObjectSummaries();
  }

  /***
   * Creates a new Amazon EC2 client to invoke service methods on Amazon EC2
   *
   * @return Amazon EC2 client to invoke service methods on Amazon EC2
   */
  public AmazonEC2 getEc2Client() {
    return amazonEC2Supplier.get();
  }

  /***
   * Creates a new Amazon AutoScaling client to invoke service methods on Amazon AutoScaling
   *
   * @return Amazon AutoScaling client to invoke service methods on Amazon AutoScaling
   */
  public AmazonAutoScaling getAmazonAutoScalingClient() {
    return amazonAutoScalingSupplier.get();
  }

  /***
   * Creates a new Amazon S3 client to invoke service methods on Amazon S3
   *
   * @return Amazon S3 client to invoke service methods on Amazon S3
   */
  public AmazonS3 getS3Client() {
    return amazonS3Supplier.get();
  }
}
