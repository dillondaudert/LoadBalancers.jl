using Base.Test
include("helper.jl")

@testset "helper.jl Tests" begin
    
    @testset "Single process" begin
        @test w_idx(myid()) == myid()
        let msg_chls = create_msg_chls(16)
            @test length(msg_chls) == 1
            @test msg_chls[1].whence == myid()
            @test msg_chls[1].where == myid()
            @test msg_chls[w_idx(myid())] == msg_chls[1]
        end
    end

    @testset "Multiprocess" begin
        @testset "1 Controller, 1 Worker Tests" begin
            addprocs(1)
            @spawnat 2 include("helper.jl")
            @test w_idx(myid()) == 0
            let msg_chls = create_msg_chls(16)
                @test length(msg_chls) == 1
                @test msg_chls[1].whence == myid()
                @test msg_chls[1].where == workers()[1]
                @test_throws BoundsError msg_chls[w_idx(myid())]
                #
                @test remotecall_fetch(length, workers()[1], msg_chls) == 1
                @test remotecall_fetch((y)->(x = get_msg_chl(myid(), y); x.where), 
                                       workers()[1], 
                                       msg_chls) == workers()[1]
            end
            rmprocs(workers()[1])
        end

    end


end
