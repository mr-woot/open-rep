package util;

import java.util.ResourceBundle;

/**
 * Contributed By: Tushar Mudgal
 * On: 10/6/19 | 5:12 PM
 */
public class ConfigBundle {
    private static ResourceBundle resourceBundle ;

    /**
     *
     * @return ResourceBundle
     */
    private static ResourceBundle getInstance(){
        if(resourceBundle==null){
            resourceBundle = ResourceBundle.getBundle("config");
        }
        return resourceBundle;
    }
    /**
     *
     * @return config value based on key
     */
    public static String getValue(String key){

        return ConfigBundle.getInstance().getString(key);
    }
}