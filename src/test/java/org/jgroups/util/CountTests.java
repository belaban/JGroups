package org.jgroups.util;

import org.testng.annotations.*;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Counts and dumps all tests annotated with @Test. Tests with enabled=false are excluded. Takes invocationCount
 * into account, e.g. a method with @Test(invocationCount=5) is counted 5 times.
 * @author Bela Ban
 * @since 3.1
 */
public class CountTests {

    public static void main(String[] args) throws ClassNotFoundException, IOException {
        List<Class<?>> list=Util.findClassesAnnotatedWith("org.jgroups",Test.class);
        int total=0, disabled=0;
        Map<String,Integer> group_count=new HashMap<>(list.size());
        String group=null;
        boolean details=false;

        for(int i=0; i < args.length; i++) {
            if(args[i].equals("-group")) {
                group=args[++i];
                continue;
            }
            if(args[i].equals("-details")) {
                details=true;
                continue;
            }

            System.out.println("CountTests [-group groupname] [-details]");
            return;
        }

        for(Class<?> clazz: list) {
            Test annotation=clazz.getAnnotation(Test.class);
            if(!annotation.enabled()) {
                disabled++;
                continue;
            }

            String name=clazz.getSimpleName();
            if(details && group != null && hasGroup(annotation, group))
                System.out.println(Util.bold("\n" + name + ":"));
            Method[] methods=clazz.getDeclaredMethods();
            int cnt=0;
            for(Method method: methods) {
                int mods=method.getModifiers();
                if(!Modifier.isPublic(mods))
                    continue;

                if(hasAnnotations(method, BeforeSuite.class, BeforeTest.class, BeforeClass.class, BeforeMethod.class,
                                  BeforeGroups.class, AfterSuite.class, AfterClass.class, AfterTest.class, AfterMethod.class,
                                  AfterGroups.class))
                    continue;

                Test method_annotation=method.getAnnotation(Test.class);
                if(method_annotation != null) {
                    if(method_annotation.enabled() == false) {
                        disabled++;
                        continue;
                    }
                    int invocation_cnt=method_annotation.invocationCount();
                    if(invocation_cnt > 1) {
                        if(details && group != null && hasGroup(annotation, group))
                            System.out.println("    " + method.getName() + " [invocationCount=" + invocation_cnt + "]");
                        total+=invocation_cnt;
                        cnt+=invocation_cnt;
                        continue;
                    }
                }
                if(details && group != null && hasGroup(annotation, group))
                    System.out.println("    " + method.getName());
                total++;
                cnt++;
            }
            if(details && group != null && hasGroup(annotation, group))
                System.out.println("    (" + cnt + ")");

            String[] groups=annotation.groups();
            if(groups != null) {
                for(String grp: groups) {
                    if(group != null && !grp.equals(group))
                        continue;
                    Integer count=group_count.get(grp);
                    if(count == null)
                        group_count.put(grp, cnt);
                    else
                        group_count.put(grp, count+cnt);
                }
            }
        }


        System.out.println("\nFound " + total + " classes annotated with @Test: " + disabled + " disabled, groups:");
        for(Map.Entry<String,Integer> entry: group_count.entrySet()) {
            String key=entry.getKey();
            Integer val=entry.getValue();
            System.out.println(key + ": " + val);
        }
    }

    protected static boolean hasAnnotations(Method method, Class<?> ... annotations) {
        if(method == null || annotations == null)
            return false;
        for(Class<?> annotation: annotations) {
            Class<Annotation> ann=(Class<Annotation>)annotation;
            if(method.getAnnotation(ann) != null)
                return true;
        }
        return false;
    }

    protected static boolean hasGroup(Test annotation, String grp) {
        String[] groups=annotation.groups();
        if(groups != null) {
            for(String group: groups) {
                if(group.equals(grp))
                    return true;
            }
        }
        return false;
    }
}
