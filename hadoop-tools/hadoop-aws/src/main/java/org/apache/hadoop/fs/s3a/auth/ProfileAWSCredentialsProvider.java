package org.apache.hadoop.fs.s3a.auth;

import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.profiles.ProfileFile;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import org.apache.commons.lang3.SystemUtils;
import org.apache.hadoop.conf.Configuration;

import java.net.URI;
import java.nio.file.FileSystems;
import java.nio.file.Path;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public class ProfileAWSCredentialsProvider extends AbstractAWSCredentialProvider {
    public static final String NAME
            = "org.apache.hadoop.fs.s3a.auth.ProfileAWSCredentialsProvider";
    public static final String PROFILE_FILE = "fs.s3a.auth.profile.file";
    public static final String PROFILE_NAME = "fs.s3a.auth.profile.name";

    private final ProfileCredentialsProvider pcp;

    private static Path getCredentialsPath(Configuration conf) {
        String credentialsFile = conf.get(PROFILE_FILE, null);
        if (credentialsFile == null) {
            credentialsFile = SystemUtils.getEnvironmentVariable("AWS_SHARED_CREDENTIALS_FILE", null);
        }
        Path path = (credentialsFile == null) ?
                FileSystems.getDefault().getPath(SystemUtils.getUserHome().getPath(),".aws","credentials")
                : FileSystems.getDefault().getPath(credentialsFile);
        return path;
    }

    private static String getCredentialsName(Configuration conf) {
        String profileName = conf.get(PROFILE_NAME, null);
        if (profileName == null) {
            profileName = SystemUtils.getEnvironmentVariable("AWS_PROFILE", "default");
        }
        return profileName;
    }

    public ProfileAWSCredentialsProvider(URI uri, Configuration conf) {
        super(uri, conf);
        ProfileCredentialsProvider.Builder builder = ProfileCredentialsProvider.builder();
        builder.profileName(getCredentialsName(conf)).profileFile(ProfileFile.builder().content(getCredentialsPath(conf)).type(ProfileFile.Type.CREDENTIALS).build());
        pcp = builder.build();
    }

    public AwsCredentials resolveCredentials() {
        return pcp.resolveCredentials();
    }
}
