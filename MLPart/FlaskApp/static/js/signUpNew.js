$(function() {
    $('#btnSignUp').click(function() {
 
        $.ajax({
            url: '/signUpNew',
            data: $('form').serialize(),
            type: 'POST',
            success: function(response) {
                alert(response);
            },
            error: function(error) {
                console.log(error);
            }
        });
    });
})