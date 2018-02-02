package org.apache.accumulo.util;

import org.apache.AIS;

public enum Version {

    ONE_SEVEN_THREE("1.7.3"),
    ONE_SEVEN_FOUR("1.7.4-SNAPSHOT"),
    ONE_EIGHT_ONE("1.8.1"),
    ONE_EIGHT_TWO("1.9.0-SNAPSHOT"),
    TWO_ZERO_ZERO("2.0.0");

    private String verStr;
    Version(String verStr) {
        this.verStr = verStr;
    }

    public static Version getVersion(String s) {
        for (Version v : Version.values()) {
            if(v.verStr.equals(s))
                return v;
        }
        throw new IllegalArgumentException("No Version found for " + s);
    }

    public static void isRequiredVersion(Version... requiredVersions) throws Exception {
        Version provided = Version.getVersion(AIS.VERSION);
        boolean found = false;
        StringBuilder list = new StringBuilder();
        for (Version required : requiredVersions) {
            list.append(required.verStr).append(" ");
            if(required.equals(provided))
                found = true;
        }
        if(!found)
            throw new Exception("Test was run with version " + AIS.VERSION + " but required one of the following versions: " + list.toString());
    }
}