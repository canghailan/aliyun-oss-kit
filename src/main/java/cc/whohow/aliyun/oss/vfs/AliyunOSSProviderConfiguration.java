package cc.whohow.aliyun.oss.vfs;

import cc.whohow.vfs.configuration.ProviderConfiguration;

import java.util.List;

public class AliyunOSSProviderConfiguration extends ProviderConfiguration {
    private List<Profile> profiles;
    private List<Cname> cnames;

    public List<Profile> getProfiles() {
        return profiles;
    }

    public void setProfiles(List<Profile> profiles) {
        this.profiles = profiles;
    }

    public List<Cname> getCnames() {
        return cnames;
    }

    public void setCnames(List<Cname> cnames) {
        this.cnames = cnames;
    }

    static class Profile {
        private String accessKeyId;
        private String secretAccessKey;

        public String getAccessKeyId() {
            return accessKeyId;
        }

        public void setAccessKeyId(String accessKeyId) {
            this.accessKeyId = accessKeyId;
        }

        public String getSecretAccessKey() {
            return secretAccessKey;
        }

        public void setSecretAccessKey(String secretAccessKey) {
            this.secretAccessKey = secretAccessKey;
        }
    }

    static class Cname {
        private String uri;
        private String cname;

        public String getUri() {
            return uri;
        }

        public void setUri(String uri) {
            this.uri = uri;
        }

        public String getCname() {
            return cname;
        }

        public void setCname(String cname) {
            this.cname = cname;
        }
    }
}
