// $Id: MethodLookupClos.java,v 1.2 2003/09/24 23:20:46 belaban Exp $

package org.jgroups.blocks;


import java.lang.reflect.Method;
import java.util.Vector;




public class MethodLookupClos implements MethodLookup {

    static Class boolean_type=Boolean.class;
    static Class char_type=Character.class;
    static Class byte_type=Byte.class;
    static Class short_type=Short.class;
    static Class int_type=Integer.class;
    static Class long_type=Long.class;
    static Class float_type=Float.class;
    static Class double_type=Double.class;


    public Method findMethod(Class target_class, String method_name, Vector args) throws Exception {
        Method retval=null, method;
        Method[] methods=target_class.getMethods();
        Vector matching_methods=new Vector();  // contains all possible matches
        int num_args=args.size();
        int rank=1000, new_rank=0;



        /* 1. Match by name and number of parameters */

        for(int i=0; i < methods.length; i++) {
            method=methods[i];
            if(method.getName().equals(method_name) &&
                    method.getParameterTypes().length == num_args) {
                matching_methods.addElement(method);
                continue;
            }
        }

        if(matching_methods.size() == 1)
            return (Method)matching_methods.elementAt(0);
        else
            if(matching_methods.size() < 1)
                throw new NoSuchMethodException();


        /* 2. If this is not enough (more than 1 method matching), match formal parameters
           with actual arguments. Discard methods whose arguments cannot be cast to the
           formal parameters */

        for(int i=0; i < matching_methods.size(); i++) {
            method=(Method)matching_methods.elementAt(i);
            new_rank=matchParameters(method, args);
            if(new_rank < 0)
                continue;
            if(new_rank <= rank) {  // Discards duplicate methods ! But we don't care ...
                retval=method;
                rank=new_rank;
            }
        }

        if(retval != null)
            return retval;
        else
            throw new NoSuchMethodException();
    }


    int computeDistance(Class from, Class to) {
        int retval=0;
        Class current=from;

        while(current != null) {
            current=current.getSuperclass();
            if(current == null)
                break;
            else
                if(current.equals(to))
                    return ++retval;
                else
                    retval++;
        }
        return 0;
    }


    /**
     Returns -1 if arg cannot be unwrapped to a primitive *and* if conversion from
     unwrapped arg to primitive is not allowed. Otherwise, returns the distance from primitive
     to unwrapped arg (0 means equals, 1 e.g. for distance from 'int' to 'long' etc.
     These are widening promotions, Java grammar 5.1.2
     */
    int computeDistanceFromPrimitive(Class primitive, Class arg) {
        if(primitive == Boolean.TYPE) {
            if(arg == boolean_type)
                return 0;
            else
                return -1;
        }
        if(primitive == Character.TYPE) {
            if(arg == char_type)
                return 0;
            else
                return -1;
        }
        if(primitive == Byte.TYPE) {
            if(arg == byte_type)
                return 0;
            else
                return -1;
        }
        if(primitive == Short.TYPE) {
            if(arg == byte_type || arg == short_type || arg == int_type || arg == long_type ||
                    arg == float_type || arg == double_type) {
                if(arg == short_type) return 0;
                if(arg == byte_type) return 1;
                if(arg == int_type) return 2;
                if(arg == long_type) return 3;
                if(arg == float_type) return 4;
                if(arg == double_type) return 5;
            }
            else
                return -1;
        }
        if(primitive == Integer.TYPE) {
            if(arg == byte_type || arg == short_type || arg == int_type || arg == long_type ||
                    arg == float_type || arg == double_type) {
                if(arg == int_type) return 0;
                if(arg == byte_type) return 1;
                if(arg == short_type) return 2;
                if(arg == long_type) return 3;
                if(arg == float_type) return 4;
                if(arg == double_type) return 5;
            }
            else
                return -1;
        }
        if(primitive == Long.TYPE) {
            if(arg == byte_type || arg == short_type || arg == int_type || arg == long_type ||
                    arg == float_type || arg == double_type) {
                if(arg == long_type) return 0;
                if(arg == byte_type) return 1;
                if(arg == int_type) return 2;
                if(arg == short_type) return 3;
                if(arg == float_type) return 4;
                if(arg == double_type) return 5;
            }
            else
                return -1;
        }
        if(primitive == Float.TYPE) {
            if(arg == byte_type || arg == short_type || arg == int_type || arg == long_type ||
                    arg == float_type || arg == double_type) {
                if(arg == float_type) return 0;
                if(arg == byte_type) return 1;
                if(arg == int_type) return 2;
                if(arg == long_type) return 3;
                if(arg == short_type) return 4;
                if(arg == double_type) return 5;
            }
            else
                return -1;
        }
        if(primitive == Double.TYPE) {
            if(arg == byte_type || arg == short_type || arg == int_type || arg == long_type ||
                    arg == float_type || arg == double_type) {
                if(arg == double_type) return 0;
                if(arg == byte_type) return 1;
                if(arg == short_type) return 2;
                if(arg == int_type) return 3;
                if(arg == long_type) return 4;
                if(arg == float_type) return 5;
            }
            else
                return -1;
        }

        return -1;
    }



    /* Assume that args is non-null and that method and args are same size.
       Matches arguments with formal parameters according to 'proximity' of argument
       to parameter, e.g. an exact match yields 0, a direct subtype yields 1, and an
       indirect subtype <n> where <n> is the distance (e.g. 3). No match returns -1.
       The best match is the one with the lowest return value */

    int matchParameters(Method method, Vector args) {
        Class[] formal_parms=method.getParameterTypes();
        Class formal_parm;
        Object arg;
        int retval=0;

        for(int i=0; i < formal_parms.length; i++) {
            arg=args.elementAt(i);
            formal_parm=formal_parms[i];

            if(formal_parm.isPrimitive()) {
                int distance=computeDistanceFromPrimitive(formal_parm, arg.getClass());
                if(distance < 0)
                    return -1;
                else
                    retval+=distance;
                continue;
            }

            if(formal_parm.equals(arg.getClass())) {      // exact match argument with formal parm
                retval+=0;
                continue;
            }

            if(formal_parm.isInstance(arg)) {             // is arg a subtype of formal_class ?
                int t=computeDistance(arg.getClass(), formal_parm);
                return t;
            }
            else                                          // no match
                return -1;
        }
        return retval;
    }


//      Class getClassForPrimitive(Class primitive) {
//  	if(primitive == null) return null;
//  	if(!primitive.isPrimitive()) {
//  	    System.err.println("MethodLookupClos.getClassForPrimitive(): arg " +
//  			       primitive + " is not a primitive !");
//  	    return null;
//  	}

//  	if(primitive == Boolean.TYPE)
//  	    return boolean_type;
//  	if(primitive == Character.TYPE)
//  	    return char_type;
//  	if(primitive == Byte.TYPE)
//  	    return byte_type;
//  	if(primitive == Short.TYPE)
//  	    return short_type;
//  	if(primitive == Integer.TYPE)
//  	    return int_type;
//  	if(primitive == Long.TYPE)
//  	    return long_type;
//  	if(primitive == Float.TYPE)
//  	    return float_type;
//  	if(primitive == Double.TYPE)
//  	    return double_type;

//  	return null;
//      }


}
