package com.test;

import org.junit.Assert;
import org.junit.Test;

public class TestString {


    @Test
    public void testString(){
        String str = "myFirstName";
        String s = convertString(str);
        Assert.assertEquals("my_first_name",s);

        str = "OnlineUsers";
        s = convertString(str);
        Assert.assertEquals("online_users",s);

        str = "Address";
        s = convertString(str);
        Assert.assertEquals("address",s);

        str = "validHTTPRequest";
        s = convertString(str);
        Assert.assertEquals("valid_http_request",s);
    }

    public String convertString(String origin){
        StringBuilder sb = new StringBuilder();
        boolean isSame = false;
        char before = 0;
        char after = 0;
        for(int i=0;i<=origin.length()-1;i++){
            before = origin.charAt(i);
            if(i != origin.length()-1){
                after = origin.charAt(i+1);
            }

            if(before >= 'a' && before <= 'z' && after >= 'A' && after <= 'Z' ){
                isSame = false;
                sb.append(convertToLowercase(before)).append('_');
            }else if(isSame && before >= 'A' && before <= 'Z' && after >= 'a' && after <= 'z'){
                isSame = false;
                sb.append('_').append(convertToLowercase(before));
            }else if(before >= 'A' && before <= 'Z' && after >= 'A' && after <= 'Z'){
                isSame = true;
                sb.append(convertToLowercase(before));
            } else
                sb.append(convertToLowercase(before));
            }

        return sb.toString();
    }

    public char convertToLowercase(char c){
        if(c >= 'A' && c <='Z'){
            return (char)(c+32);
        }
        return c;
    }

}
