package org.apache.hadoop.fs.s3a;

import org.apache.commons.lang3.SystemUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.profiles.ProfileFile;

import java.net.URI;
import java.nio.file.FileSystems;
import java.nio.file.Path;

@InterfaceAudience.Public
@InterfaceStability.Stable
public class ProfileAWSCredentialsProvider implements AwsCredentialsProvider {
    public static final String NAME
            = "org.apache.hadoop.fs.s3a.ProfileAWSCredentialsProvider";

    private ProfileCredentialsProvider pcp;

    private static Path getCredentialsPath() {
        String credentialsFile = SystemUtils.getEnvironmentVariable("AWS_SHARED_CREDENTIALS_FILE", null);
        Path path = (credentialsFile == null) ?
                FileSystems.getDefault().getPath(SystemUtils.getUserHome().getPath(),".aws","credentials")
                : FileSystems.getDefault().getPath(credentialsFile);
        return path;
    }

    public ProfileAWSCredentialsProvider(URI uri, Configuration conf) {
        ProfileCredentialsProvider.Builder builder = ProfileCredentialsProvider.builder();
        builder.profileName("default").profileFile(ProfileFile.builder().content(getCredentialsPath()).type(ProfileFile.Type.CREDENTIALS).build());
        pcp = builder.build();
    }

    public ProfileAWSCredentialsProvider(Configuration conf) {
        ProfileCredentialsProvider.Builder builder = ProfileCredentialsProvider.builder();
        builder.profileName("default").profileFile(ProfileFile.builder().content(getCredentialsPath()).build());
        pcp = builder.build();
    }
    public AwsCredentials resolveCredentials() {
        return pcp.resolveCredentials();
    }
}
