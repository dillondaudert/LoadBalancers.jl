using Base.Test
include("utils.jl")


@testset "WorkUnit Tests" begin

@testset "WorkUnit constructor tests"  begin
    @test_throws ErrorException WorkUnit(-1, 2)
    @test_throws ErrorException WorkUnit(2, 0)
    @test_throws ErrorException WorkUnit(0, 0)
    @test typeof(WorkUnit(2, 3)) == WorkUnit{typeof(2)}
end

@testset "split_work tests" begin
    testwork = WorkUnit(10, 3)
    testwork2 = WorkUnit(555, 4)
    testwork3 = WorkUnit(1, 1)

    @testset "split_work domain tests" begin
        @test_throws DomainError split_work(testwork, -0.5)
        @test_throws DomainError split_work(testwork2, 1.63)
        @test_throws DomainError split_work(testwork3, .5)
    end

    @testset "split_work 50% test" begin
        u1, v1 = split_work(testwork, 0.5)
        u2, v2 = split_work(testwork2, 0.5)
        @test u1.units == 5
        @test v1.units == 5
        @test u2.units == 278
        @test v2.units == 277
    end

    @testset "split_work 0 + ϵ test" begin
        u1, v1 = split_work(testwork, eps(Float64))
        u2, v2 = split_work(testwork2, eps(Float64))
        @test u1.units == 1
        @test v1.units == 9
        @test u2.units == 1
        @test v2.units == 554
    end

    @testset "split_work 1 - ϵ test" begin
        u1, v1 = split_work(testwork, 1 - eps(Float64))
        u2, v2 = split_work(testwork2, 1 - eps(Float64))
        @test u1.units == 9
        @test v1.units == 1
        @test u2.units == 554
        @test v2.units == 1
    end

end

end

@testset "Utility Tests" begin
    
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
            @spawnat 2 include("utils.jl")
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
