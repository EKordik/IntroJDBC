Instructions:
----------------
In lab1 we demonstrated the planning and thought process involved in designing
a flexible database access system based on the SOA architectural style.

For this lab you should design your own system for your garage project. You
may not copy and paste any code from lab1 or from db.accessor. Please study
the samples so that you can write your own code!

Start simple and start at the back. Create your own low-level database 
access strategy object with just one method -- something easy like a simple
query. Test that method, then move on to the DAO.

Now create a DAO strategy object for a specific purpose -- say for 
Garage data. Create one method that talks to the method you created in the
low-level object. Test your DAO and move on to the service class.

Now create a service object for a specific purpose -- say for Garage services.
Create one method that talks to the DAO method and test this method. When
this test passes you've completed a simple SOA architecture. Now just keep
adding more methods until you have everything you need. What do you need?
That's for you to decide.